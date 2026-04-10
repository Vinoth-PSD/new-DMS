from celery import shared_task
from django.core.files.base import ContentFile
from django.utils import timezone
from pypdf import PdfReader, PdfWriter
import io
import hashlib
from .models import AuditLog, Document, DocumentPage
from .services import assign_unassigned_pages, split_document_pages


def _merge_docx_byte_segments(parts: list[bytes]) -> bytes:
    """Append multiple .docx files into one document (in order)."""
    from docx import Document
    from docxcompose.composer import Composer

    master = Document(io.BytesIO(parts[0]))
    composer = Composer(master)
    for payload in parts[1:]:
        composer.append(Document(io.BytesIO(payload)))
    out = io.BytesIO()
    composer.save(out)
    return out.getvalue()


@shared_task
def split_document_task(document_id: int) -> dict:
    document = Document.objects.get(id=document_id)
    total_pages = split_document_pages(document)
    if document.total_pages != total_pages:
        document.total_pages = total_pages
        document.save(update_fields=["total_pages", "updated_at"])
    AuditLog.objects.create(action=AuditLog.Action.SPLIT_DOC, document=document, metadata={})
    assigned = assign_unassigned_pages(document_id=document_id)
    return {"document_id": document_id, "pages": total_pages, "assigned": assigned}


@shared_task
def assign_pages_task(document_id: int | None = None) -> int:
    return assign_unassigned_pages(document_id=document_id)


@shared_task
def merge_document_task(document_id: int) -> dict:
    document = Document.objects.get(id=document_id)
    pages = list(document.pages.order_by("page_number"))
    if not pages:
        return {"document_id": document_id, "merged": False, "reason": "No pages to merge"}

    if any(page.status != DocumentPage.Status.COMPLETED for page in pages):
        return {"document_id": document_id, "merged": False, "reason": "All assigned pages are not completed"}

    if any(not page.processed_file for page in pages):
        return {"document_id": document_id, "merged": False, "reason": "Processed file missing for one or more pages"}

    writer = PdfWriter()
    all_pdf = all((page.processed_file.name or "").lower().endswith(".pdf") for page in pages)
    merge_segments = 1
    if all_pdf:
        for page in pages:
            with page.processed_file.open("rb") as stream:
                reader = PdfReader(stream)
                for pdf_page in reader.pages:
                    writer.add_page(pdf_page)
        output = io.BytesIO()
        writer.write(output)
        merged_name = f"merged_{document.id}_{timezone.now().strftime('%Y%m%d%H%M%S')}.pdf"
        document.final_merged_file.save(merged_name, ContentFile(output.getvalue()), save=False)
    else:
        # Non-PDF: each resource's Word upload is duplicated onto every page they own, so
        # pages 1–15 share one hash and 16–24 another. We must not pick the "most common"
        # hash (that drops whole resources). Group consecutive identical payloads in page order.
        segments: list[dict] = []
        for page in pages:
            with page.processed_file.open("rb") as stream:
                payload = stream.read()
            digest = hashlib.sha256(payload).hexdigest()
            ext = (page.processed_file.name.rsplit(".", 1)[-1] or "bin").lower()
            if not segments or digest != segments[-1]["digest"]:
                segments.append(
                    {
                        "digest": digest,
                        "payload": payload,
                        "ext": ext,
                        "page_start": page.page_number,
                        "page_end": page.page_number,
                    }
                )
            else:
                segments[-1]["page_end"] = page.page_number

        ts = timezone.now().strftime("%Y%m%d%H%M%S")
        if len(segments) == 1:
            seg = segments[0]
            merged_name = f"merged_{document.id}_{ts}.{seg['ext']}"
            document.final_merged_file.save(merged_name, ContentFile(seg["payload"]), save=False)
        else:
            exts = {s["ext"] for s in segments}
            if exts == {"docx"}:
                merged_bytes = _merge_docx_byte_segments([s["payload"] for s in segments])
                merged_name = f"merged_{document.id}_{ts}.docx"
                document.final_merged_file.save(merged_name, ContentFile(merged_bytes), save=False)
            elif exts == {"pdf"}:
                writer = PdfWriter()
                for seg in segments:
                    reader = PdfReader(io.BytesIO(seg["payload"]))
                    for pdf_page in reader.pages:
                        writer.add_page(pdf_page)
                output = io.BytesIO()
                writer.write(output)
                merged_name = f"merged_{document.id}_{ts}.pdf"
                document.final_merged_file.save(merged_name, ContentFile(output.getvalue()), save=False)
            elif exts == {"doc"}:
                return {
                    "document_id": document_id,
                    "merged": False,
                    "reason": "Merging multiple legacy .doc segments into one file is not supported; use .docx for all resources.",
                }
            else:
                return {
                    "document_id": document_id,
                    "merged": False,
                    "reason": "Cannot merge multiple segments with mixed file types into one document.",
                }
        merge_segments = len(segments)
    document.status = Document.Status.COMPLETED
    document.merged_at = timezone.now()
    document.save(update_fields=["final_merged_file", "status", "merged_at", "updated_at"])
    AuditLog.objects.create(
        action=AuditLog.Action.MERGE_DOC,
        document=document,
        metadata={"pages_merged": len(pages), "merge_segments": merge_segments},
    )
    return {"document_id": document_id, "merged": True, "merged_file": document.final_merged_file.url}
