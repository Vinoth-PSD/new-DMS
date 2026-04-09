from celery import shared_task
from django.core.files.base import ContentFile
from django.utils import timezone
from pypdf import PdfReader, PdfWriter
import io
import hashlib
from .models import AuditLog, Document, DocumentPage
from .services import assign_unassigned_pages, split_document_pages


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
        # For non-PDF workflows, always emit ONE merged artifact (never per-page zip).
        # We pick a canonical bundled payload:
        # 1) most frequent content hash across pages
        # 2) tie-break by earliest page number.
        payloads_by_hash: dict[str, dict] = {}
        for page in pages:
            with page.processed_file.open("rb") as stream:
                payload = stream.read()
            digest = hashlib.sha256(payload).hexdigest()
            ext = (page.processed_file.name.rsplit(".", 1)[-1] or "bin").lower()
            bucket = payloads_by_hash.get(digest)
            if not bucket:
                payloads_by_hash[digest] = {
                    "count": 1,
                    "payload": payload,
                    "ext": ext,
                    "first_page": page.page_number,
                }
            else:
                bucket["count"] += 1
                bucket["first_page"] = min(bucket["first_page"], page.page_number)

        chosen = sorted(
            payloads_by_hash.values(),
            key=lambda b: (-b["count"], b["first_page"]),
        )[0]
        merged_name = f"merged_{document.id}_{timezone.now().strftime('%Y%m%d%H%M%S')}.{chosen['ext']}"
        document.final_merged_file.save(merged_name, ContentFile(chosen["payload"]), save=False)
    document.status = Document.Status.COMPLETED
    document.merged_at = timezone.now()
    document.save(update_fields=["final_merged_file", "status", "merged_at", "updated_at"])
    AuditLog.objects.create(action=AuditLog.Action.MERGE_DOC, document=document, metadata={"pages_merged": len(pages)})
    return {"document_id": document_id, "merged": True, "merged_file": document.final_merged_file.url}
