import io
import zipfile
from pathlib import Path
from datetime import timedelta
from xml.etree import ElementTree
from django.core.files.base import ContentFile
from django.db import transaction
from django.utils import timezone
from pypdf import PdfReader, PdfWriter
from .models import AssignmentQueue, AuditLog, Document, DocumentPage, ResourceProfile

ONLINE_TTL_SECONDS = 60


def update_document_status(document: Document) -> None:
    if document.is_on_hold:
        document.status = Document.Status.ON_HOLD
        document.save(update_fields=["status", "updated_at"])
        return
    page_statuses = set(document.pages.values_list("status", flat=True))
    if not page_statuses:
        document.status = Document.Status.NOT_ASSIGNED
    elif DocumentPage.Status.ON_HOLD in page_statuses:
        document.status = Document.Status.ON_HOLD
    elif page_statuses == {DocumentPage.Status.COMPLETED}:
        document.status = Document.Status.COMPLETED
    elif DocumentPage.Status.IN_PROGRESS in page_statuses:
        document.status = Document.Status.IN_PROGRESS
    elif DocumentPage.Status.ASSIGNED in page_statuses:
        document.status = Document.Status.ASSIGNED
    elif DocumentPage.Status.PENDING_APPROVAL in page_statuses:
        document.status = Document.Status.PENDING_APPROVAL
    else:
        document.status = Document.Status.NOT_ASSIGNED
    document.save(update_fields=["status", "updated_at"])


@transaction.atomic
def assign_unassigned_pages(document_id: int | None = None) -> int:
    cutoff = timezone.now() - timedelta(seconds=ONLINE_TTL_SECONDS)
    resources = [
        r
        for r in ResourceProfile.objects.select_for_update()
        .filter(is_active_session=True, last_seen_at__gte=cutoff)
        .order_by("id")
        if r.remaining_capacity > 0
    ]
    unassigned_pages = (
        DocumentPage.objects.select_for_update()
        .filter(status=DocumentPage.Status.NOT_ASSIGNED, assigned_to__isnull=True, document__is_on_hold=False)
        .order_by("document_id", "page_number")
    )
    if document_id:
        unassigned_pages = unassigned_pages.filter(document_id=document_id)

    # No active resources: keep pages queued for later auto-assignment.
    if not resources:
        for page in unassigned_pages:
            AssignmentQueue.objects.get_or_create(
                page=page, defaults={"reason": AssignmentQueue.Reason.NO_ACTIVE_RESOURCE}
            )
        return 0

    assigned_count = 0
    touched_doc_ids: set[int] = set()

    pages_iter = iter(unassigned_pages)
    exhausted = False
    for resource in resources:
        slots = max(resource.remaining_capacity, 0)
        while slots > 0 and not exhausted:
            try:
                page = next(pages_iter)
            except StopIteration:
                exhausted = True
                break
            touched_doc_ids.add(page.document_id)
            page.assigned_to = resource
            page.assigned_at = timezone.now()
            page.status = DocumentPage.Status.ASSIGNED
            page.save(update_fields=["assigned_to", "assigned_at", "status", "updated_at"])
            AssignmentQueue.objects.filter(page=page).delete()
            AuditLog.objects.create(
                actor=None,
                action=AuditLog.Action.ASSIGN_PAGE,
                document=page.document,
                page=page,
                metadata={"resource_id": resource.id},
            )
            assigned_count += 1
            slots -= 1

    # Any remaining pages couldn't be assigned due to exhausted capacity.
    for page in pages_iter:
        touched_doc_ids.add(page.document_id)
        AssignmentQueue.objects.get_or_create(
            page=page, defaults={"reason": AssignmentQueue.Reason.NO_CAPACITY}
        )

    for doc in Document.objects.filter(id__in=touched_doc_ids):
        update_document_status(doc)
    return assigned_count


def mark_download_started(page: DocumentPage, actor_id: int) -> None:
    if page.status in [DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS]:
        page.status = DocumentPage.Status.IN_PROGRESS
        page.download_started_at = timezone.now()
        page.save(update_fields=["status", "download_started_at", "updated_at"])
        AuditLog.objects.create(
            actor_id=actor_id,
            action=AuditLog.Action.DOWNLOAD_PAGE,
            document=page.document,
            page=page,
            metadata={},
        )
        update_document_status(page.document)


def detect_file_type(filename: str) -> str:
    extension = Path(filename).suffix.lower()
    if extension == ".pdf":
        return Document.FileType.PDF
    if extension == ".docx":
        return Document.FileType.DOCX
    raise ValueError("Only PDF and DOCX files are allowed.")


def get_total_pages(document: Document) -> int:
    if document.file_type == Document.FileType.PDF:
        with document.original_file.open("rb") as stream:
            return len(PdfReader(stream).pages)
    return _count_docx_pages(document)


def split_document_pages(document: Document) -> int:
    if document.file_type == Document.FileType.PDF:
        return _split_pdf_document(document)
    return _split_docx_document(document)


def _split_pdf_document(document: Document) -> int:
    with document.original_file.open("rb") as stream:
        reader = PdfReader(stream)
        total = len(reader.pages)
        for page_no in range(1, total + 1):
            page, _ = DocumentPage.objects.get_or_create(document=document, page_number=page_no)
            writer = PdfWriter()
            writer.add_page(reader.pages[page_no - 1])
            output = io.BytesIO()
            writer.write(output)
            page.split_file.save(
                f"{document.id}_page_{page_no}.pdf",
                ContentFile(output.getvalue()),
                save=False,
            )
            page.save(update_fields=["split_file", "updated_at"])
    return total


def _split_docx_document(document: Document) -> int:
    total = _count_docx_pages(document)
    with document.original_file.open("rb") as stream:
        source_bytes = stream.read()
    for page_no in range(1, total + 1):
        page, _ = DocumentPage.objects.get_or_create(document=document, page_number=page_no)
        page.split_file.save(
            f"{document.id}_page_{page_no}.docx",
            ContentFile(source_bytes),
            save=False,
        )
        page.save(update_fields=["split_file", "updated_at"])
    return total


def _count_docx_pages(document: Document) -> int:
    with zipfile.ZipFile(document.original_file.path) as zf:
        app_xml = zf.read("docProps/app.xml")
        root = ElementTree.fromstring(app_xml)
        pages_node = root.find(".//{*}Pages")
        if pages_node is not None and pages_node.text and pages_node.text.isdigit():
            return max(int(pages_node.text), 1)

        # Fallback: estimate by explicit Word page-break markers.
        document_xml = zf.read("word/document.xml").decode("utf-8", errors="ignore")
    return max(document_xml.count('w:type="page"') + 1, 1)
