import io
import mimetypes
import zipfile
import re
import hashlib
import json
from datetime import date
from pathlib import Path
from urllib.parse import quote
from django.conf import settings
from django.db.models import Count, Q
from django.utils import timezone
from django.contrib.auth.models import User
from django.shortcuts import get_object_or_404
from django.http import FileResponse, HttpResponse
from django.db import transaction
from django.core.files.base import ContentFile
from rest_framework import mixins, status, viewsets
from rest_framework.decorators import action, api_view, authentication_classes, permission_classes
from rest_framework.parsers import FormParser, JSONParser, MultiPartParser
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from pypdf import PdfReader, PdfWriter
from .models import AuditLog, Document, DocumentPage, ResourceProfile
from .external_cleanup import (
    build_cleanup_filename,
    fetch_latest_job_input,
    upload_to_cleanup_dir,
)
from .external_import import sync_external_job_documents
from .merged_versioning import finalize_merged_output, suggested_merged_download_filename
from .permissions import IsResourceUser, IsStaffAdmin, IsStaffAdminOrAutomationKey
from .serializers import (
    DocumentPageSerializer,
    DocumentSerializer,
    ResourceCreateSerializer,
    ResourceSerializer,
)
from .services import mark_download_started, update_document_status
from .tasks import assign_pages_task, merge_document_task, split_document_task


def _merge_docx_payloads(parts: list[bytes]) -> bytes:
    from docx import Document as DocxDocument
    from docxcompose.composer import Composer

    master = DocxDocument(io.BytesIO(parts[0]))
    composer = Composer(master)
    for payload in parts[1:]:
        composer.append(DocxDocument(io.BytesIO(payload)))
    out = io.BytesIO()
    composer.save(out)
    return out.getvalue()


def _validate_processed_upload(document: Document, filename: str) -> tuple[str, str | None]:
    lowered = (filename or "").lower()
    is_pdf = lowered.endswith(".pdf")
    is_docx = lowered.endswith(".docx")
    is_doc = lowered.endswith(".doc")
    if not (is_pdf or is_docx or is_doc):
        return "", "Only PDF, DOCX, or DOC files are allowed."
    if document.file_type == Document.FileType.DOCX:
        if not is_docx:
            return "", "This document must be uploaded as .docx only."
        return "docx", None
    if document.file_type == Document.FileType.PDF:
        if not is_pdf:
            return "", "This document must be uploaded as .pdf only."
        return "pdf", None
    return ("pdf" if is_pdf else ("docx" if is_docx else "doc")), None




class AdminDashboardViewSet(viewsets.ViewSet):
    permission_classes = [IsStaffAdmin]

    def list(self, request):
        total_documents = Document.objects.count()
        total_resources = ResourceProfile.objects.count()
        documents_processing_now = Document.objects.filter(status=Document.Status.IN_PROGRESS).count()
        pending_approval = DocumentPage.objects.filter(status=DocumentPage.Status.PENDING_APPROVAL).count()
        assigned_pages = DocumentPage.objects.filter(status=DocumentPage.Status.ASSIGNED).count()
        unassigned_pages = DocumentPage.objects.filter(status=DocumentPage.Status.NOT_ASSIGNED).count()
        unassigned_documents = (
            Document.objects.annotate(
                unassigned_count=Count("pages", filter=Q(pages__status=DocumentPage.Status.NOT_ASSIGNED))
            )
            .filter(unassigned_count__gt=0)
            .count()
        )
        return Response(
            {
                "total_documents": total_documents,
                "total_resources": total_resources,
                "documents_processing_now": documents_processing_now,
                "pending_approval": pending_approval,
                "assigned_pages": assigned_pages,
                "unassigned_pages": unassigned_pages,
                "unassigned_documents": unassigned_documents,
            }
        )


class ResourceViewSet(viewsets.ModelViewSet):
    queryset = ResourceProfile.objects.select_related("user").all()
    permission_classes = [IsStaffAdmin]

    def get_serializer_class(self):
        if self.action == "create":
            return ResourceCreateSerializer
        return ResourceSerializer

    @action(detail=True, methods=["post"], url_path="manual-upload-toggle")
    def manual_upload_toggle(self, request, pk=None):
        profile = self.get_object()
        enabled = request.data.get("enabled")
        if isinstance(enabled, str):
            enabled = enabled.strip().lower() in {"1", "true", "yes", "on"}
        profile.manual_upload_enabled = bool(enabled)
        profile.save(update_fields=["manual_upload_enabled", "updated_at"])
        return Response({"id": profile.id, "manual_upload_enabled": profile.manual_upload_enabled})

    @action(detail=True, methods=["post"], url_path="break-toggle")
    def break_toggle(self, request, pk=None):
        profile = self.get_object()
        enabled = request.data.get("enabled")
        if isinstance(enabled, str):
            enabled = enabled.strip().lower() in {"1", "true", "yes", "on"}
        profile.set_break(bool(enabled))
        # Preserve existing assignments; only pause NEW auto-assignment while on break.
        if not profile.is_on_break and profile.remaining_capacity > 0:
            assign_pages_task.delay()
        return Response(
            {
                "id": profile.id,
                "is_on_break": profile.is_on_break,
                "break_started_at": profile.break_started_at,
                "break_ended_at": profile.break_ended_at,
                "total_break_seconds": profile.total_break_seconds,
            }
        )


class DocumentViewSet(viewsets.ModelViewSet):
    queryset = Document.objects.all().order_by("-uploaded_at")
    serializer_class = DocumentSerializer
    permission_classes = [IsStaffAdmin]
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def get_permissions(self):
        if getattr(self, "action", None) == "upload_merged_corrected":
            return [IsStaffAdminOrAutomationKey()]
        return [IsStaffAdmin()]

    def get_queryset(self):
        qs = Document.objects.all().order_by("-uploaded_at")
        search = (self.request.query_params.get("search") or "").strip()
        status_filter = (self.request.query_params.get("status") or "").strip()
        if search:
            q = (
                Q(title__icontains=search)
                | Q(original_file__icontains=search)
                | Q(external_job_name__icontains=search)
            )
            if search.isdigit():
                q |= Q(external_job_id=int(search))
            qs = qs.filter(q)
        if status_filter and status_filter.upper() != "ALL":
            status_map = {
                "COMPLETED": Document.Status.COMPLETED,
                "ASSIGNED": Document.Status.ASSIGNED,
                "STARTED": Document.Status.IN_PROGRESS,
                "IN_PROGRESS": Document.Status.IN_PROGRESS,
                "REVIEWING": Document.Status.PENDING_APPROVAL,
                "READY FOR MERGING": Document.Status.PENDING_APPROVAL,
                "ON_HOLD": Document.Status.ON_HOLD,
            }
            mapped = status_map.get(status_filter.upper())
            if mapped:
                qs = qs.filter(status=mapped)
        return qs

    def create(self, request, *args, **kwargs):
        upload = request.FILES.get("original_file")
        force_duplicate = str(request.data.get("force_duplicate", "")).lower() in {"1", "true", "yes"}
        if upload and not force_duplicate:
            original_name = upload.name.strip()
            base_title = original_name.rsplit(".", 1)[0]
            dup = Document.objects.filter(
                Q(original_file__icontains=original_name) | Q(title__iexact=base_title)
            ).order_by("-uploaded_at")
            if dup.exists():
                latest = dup.first()
                return Response(
                    {
                        "duplicate_detected": True,
                        "detail": "Duplicate document name detected.",
                        "existing_document_id": latest.id,
                        "existing_uploaded_at": latest.uploaded_at,
                    },
                    status=status.HTTP_409_CONFLICT,
                )
        return super().create(request, *args, **kwargs)

    def perform_create(self, serializer):
        document = serializer.save(uploaded_by=self.request.user)
        split_document_task.delay(document.id)

    @action(detail=False, methods=["post"], url_path="sync-external")
    def sync_external(self, request):
        """
        Pull eligible input files from external MySQL + SFTP and create local Document rows
        (same storage and split/assign flow as manual upload).
        """
        # Reload .env for long-lived runserver processes so UI sync
        # uses the same latest credentials as manual CLI commands.
        try:
            from dotenv import load_dotenv

            load_dotenv(Path(settings.BASE_DIR) / ".env", override=True)
        except Exception:
            pass
        try:
            result = sync_external_job_documents(uploaded_by=request.user)
        except Exception as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_400_BAD_REQUEST)
        return Response(result, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"])
    def assign(self, request, pk=None):
        assign_pages_task.delay(document_id=int(pk))
        return Response({"queued": True}, status=status.HTTP_202_ACCEPTED)

    @action(detail=True, methods=["post"])
    def hold(self, request, pk=None):
        document = self.get_object()
        document.is_on_hold = True
        document.save(update_fields=["is_on_hold", "updated_at"])
        document.pages.filter(
            status__in=[
                DocumentPage.Status.NOT_ASSIGNED,
                DocumentPage.Status.ASSIGNED,
                DocumentPage.Status.IN_PROGRESS,
                DocumentPage.Status.REASSIGNED,
            ]
        ).update(
            status=DocumentPage.Status.ON_HOLD,
            is_on_hold=True,
            assigned_to=None,
            updated_at=timezone.now(),
        )
        update_document_status(document)
        return Response({"status": "on_hold"})

    @action(detail=True, methods=["post"])
    def unhold(self, request, pk=None):
        document = self.get_object()
        document.is_on_hold = False
        document.save(update_fields=["is_on_hold", "updated_at"])
        document.pages.filter(status=DocumentPage.Status.ON_HOLD).update(
            status=DocumentPage.Status.NOT_ASSIGNED,
            is_on_hold=False,
            assigned_to=None,
            updated_at=timezone.now(),
        )
        update_document_status(document)
        assign_pages_task.delay(document_id=document.id)
        return Response({"status": "released"})

    @action(detail=True, methods=["post"], url_path="hold-split")
    def hold_split(self, request, pk=None):
        document = self.get_object()
        page_ids = request.data.get("page_ids") or []
        rid = request.data.get("resource_profile_id")
        qs = document.pages.all()
        if page_ids:
            qs = qs.filter(id__in=page_ids)
        elif rid:
            qs = qs.filter(assigned_to_id=rid)
        else:
            return Response({"detail": "page_ids or resource_profile_id is required"}, status=400)
        updated = qs.filter(
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS, DocumentPage.Status.REASSIGNED]
        ).update(
            status=DocumentPage.Status.ON_HOLD,
            is_on_hold=True,
            assigned_to=None,
            updated_at=timezone.now(),
        )
        update_document_status(document)
        return Response({"updated": updated, "status": "split_on_hold"})

    @action(detail=True, methods=["post"], url_path="reassign-split")
    def reassign_split(self, request, pk=None):
        document = self.get_object()
        page_ids = request.data.get("page_ids") or []
        target_ids = request.data.get("resource_profile_ids") or []
        if not target_ids:
            legacy_target_id = request.data.get("resource_profile_id")
            if legacy_target_id:
                target_ids = [legacy_target_id]
        if not target_ids or not page_ids:
            return Response({"detail": "resource_profile_id(s) and page_ids are required"}, status=400)
        try:
            normalized_ids = [int(v) for v in target_ids]
        except (TypeError, ValueError):
            return Response({"detail": "Invalid resource_profile_id(s)"}, status=400)
        if len(set(normalized_ids)) != len(normalized_ids):
            return Response({"detail": "Duplicate resource_profile_ids are not allowed"}, status=400)
        targets_by_id = {r.id: r for r in ResourceProfile.objects.filter(id__in=normalized_ids)}
        if len(targets_by_id) != len(normalized_ids):
            return Response({"detail": "One or more resource_profile_ids are invalid"}, status=404)
        # Preserve client selection order for fair split assignment.
        targets = [targets_by_id[rid] for rid in normalized_ids]
        pages = list(document.pages.filter(id__in=page_ids).order_by("page_number"))
        if not pages:
            return Response({"detail": "No matching pages found for this document"}, status=404)

        target_count = len(targets)
        total_pages = len(pages)
        base = total_pages // target_count
        extra = total_pages % target_count
        allocation = [base + (1 if idx < extra else 0) for idx in range(target_count)]
        now = timezone.now()
        assignable_pages = pages
        unassigned_pages = []
        page_offset = 0
        assignments: list[dict] = []
        for idx, target in enumerate(targets):
            take = allocation[idx]
            if take <= 0:
                continue
            assigned_slice = assignable_pages[page_offset : page_offset + take]
            for page in assigned_slice:
                page.assigned_to = target
                page.assigned_at = now
                page.is_on_hold = False
                page.status = DocumentPage.Status.ASSIGNED
                page.submitted_at = None
                page.download_started_at = None
                page.save(
                    update_fields=[
                        "assigned_to",
                        "assigned_at",
                        "is_on_hold",
                        "status",
                        "submitted_at",
                        "download_started_at",
                        "updated_at",
                    ]
                )
            page_offset += take
            assignments.append(
                {
                    "resource_profile_id": target.id,
                    "assigned_pages": take,
                }
            )
        update_document_status(document)
        detail = "Split reassigned successfully."
        status_label = "reassigned"
        return Response(
            {
                "updated": len(assignable_pages),
                "requested": len(pages),
                "unassigned": len(unassigned_pages),
                "status": status_label,
                "detail": detail,
                "assignments": assignments,
            }
        )

    @action(detail=True, methods=["post"], url_path="prioritize")
    def prioritize(self, request, pk=None):
        document = self.get_object()
        resource_ids = request.data.get("resource_profile_ids") or []
        if not resource_ids:
            return Response({"detail": "resource_profile_ids is required"}, status=400)
        resources = list(
            ResourceProfile.objects.filter(id__in=resource_ids).order_by("id")
        )
        if not resources:
            return Response(
                {"detail": "No valid resources selected."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        now = timezone.now()
        document.is_urgent = True
        document.prioritized_at = now
        document.is_on_hold = False
        document.save(update_fields=["is_urgent", "prioritized_at", "is_on_hold", "updated_at"])

        # If split pages do not exist yet (e.g., celery not running), create them now
        # so urgent manual assignment can proceed immediately.
        if not document.pages.exists():
            try:
                split_document_task(document.id)
                document.refresh_from_db()
            except Exception as exc:
                return Response(
                    {"detail": f"Unable to split document before priority assignment: {exc}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

        pending_pages = list(
            document.pages.filter(
                status__in=[
                    DocumentPage.Status.NOT_ASSIGNED,
                    DocumentPage.Status.ASSIGNED,
                    DocumentPage.Status.IN_PROGRESS,
                    DocumentPage.Status.ON_HOLD,
                    DocumentPage.Status.REASSIGNED,
                ]
            ).exclude(status=DocumentPage.Status.COMPLETED).order_by("page_number")
        )
        if not pending_pages:
            update_document_status(document)
            return Response(
                {
                    "updated": 0,
                    "status": "prioritized",
                    "detail": "No assignable pages found (already completed or unavailable).",
                }
            )
        # Assign contiguous page blocks per resource (not alternating pages).
        # Example: 132 pages, 2 resources => 1-66 to first, 67-132 to second.
        total_pages = len(pending_pages)
        resource_count = len(resources)
        base = total_pages // resource_count
        extra = total_pages % resource_count
        start = 0
        for idx, target in enumerate(resources):
            take = base + (1 if idx < extra else 0)
            end = start + take
            for page in pending_pages[start:end]:
                page.assigned_to = target
                page.assigned_at = now
                page.is_on_hold = False
                page.status = DocumentPage.Status.ASSIGNED
                page.save(update_fields=["assigned_to", "assigned_at", "is_on_hold", "status", "updated_at"])
            start = end
        update_document_status(document)
        return Response({"updated": len(pending_pages), "status": "prioritized"})

    @action(detail=True, methods=["post"])
    def merge(self, request, pk=None):
        result = merge_document_task(document_id=int(pk))
        return Response(result, status=status.HTTP_200_OK if result.get("merged") else status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=["post"], url_path="cleanup-done")
    def cleanup_done(self, request, pk=None):
        """
        Pull latest source file for the job from SFTP (resolved via MySQL pl_job_file_user),
        then upload merged output to the same job's 2 Cleanup folder on SFTP.
        """
        document = self.get_object()
        job_name = (request.data.get("job_name") or "").strip()
        if not job_name:
            title = (document.title or "").strip()
            # default extraction from names like "XBSG1_something..."
            job_name = (title.split("_", 1)[0] or title).strip()
        if not job_name:
            return Response({"detail": "job_name is required."}, status=status.HTTP_400_BAD_REQUEST)
        try:
            picked = fetch_latest_job_input(job_name)
        except Exception as exc:
            return Response({"detail": f"Failed to fetch latest input from SFTP/MySQL: {exc}"}, status=400)

        if document.final_merged_file and document.final_merged_file.name:
            with document.final_merged_file.open("rb") as stream:
                payload = stream.read()
            ext = Path(document.final_merged_file.name).suffix.lower() or ".pdf"
            source_mode = "document_final_merged_file"
        else:
            payload = picked.source_payload
            ext = Path(picked.selected_source_file).suffix.lower() or ".pdf"
            source_mode = "latest_input_file_fallback"

        out_name = build_cleanup_filename(job_name, ext)
        try:
            uploaded_remote = upload_to_cleanup_dir(picked.cleanup_dir, out_name, payload)
        except Exception as exc:
            return Response({"detail": f"Failed to upload cleanup file to SFTP: {exc}"}, status=400)

        AuditLog.objects.create(
            action=AuditLog.Action.MERGE_DOC,
            document=document,
            actor=request.user if request.user.is_authenticated else None,
            metadata={
                "source": "cleanup_done",
                "job_name": job_name,
                "db_path": picked.db_path,
                "input_base_dir": picked.input_base_dir,
                "selected_source_dir": picked.selected_source_dir,
                "selected_source_file": picked.selected_source_file,
                "selected_version": picked.selected_version,
                "cleanup_dir": picked.cleanup_dir,
                "cleanup_file": uploaded_remote,
                "payload_mode": source_mode,
            },
        )
        return Response(
            {
                "status": "ok",
                "job_name": job_name,
                "selected_version": picked.selected_version,
                "selected_source_file": picked.selected_source_file,
                "cleanup_dir": picked.cleanup_dir,
                "cleanup_file": uploaded_remote,
                "payload_mode": source_mode,
            }
        )

    @action(detail=True, methods=["get"], url_path="download-final")
    def download_final(self, request, pk=None):
        """Stream merged output (PDF, DOCX, ZIP, etc.) with Content-Type from the stored filename."""
        document = self.get_object()
        field = document.final_merged_file
        if not field or not field.name:
            return Response(
                {"detail": "No merged file for this document."},
                status=status.HTTP_404_NOT_FOUND,
            )
        basename = field.name.rsplit("/", 1)[-1]
        content_type, encoding = mimetypes.guess_type(basename)
        content_type = content_type or "application/octet-stream"
        file_handle = field.open("rb")
        suggested = suggested_merged_download_filename(document)
        response = FileResponse(
            file_handle,
            content_type=content_type,
            as_attachment=True,
            filename=suggested,
        )
        response["Content-Disposition"] = (
            f'attachment; filename="{suggested}"; filename*=UTF-8\'\'{quote(suggested)}'
        )
        if encoding:
            response["Content-Encoding"] = encoding
        return response

    @action(detail=True, methods=["get"], url_path="merged-versions")
    def merged_versions(self, request, pk=None):
        document = self.get_object()
        versions = []
        for row in document.merged_version_history.order_by("version"):
            versions.append(
                {
                    "version": row.version,
                    "label": f"v{row.version}",
                    "file": row.file.url if row.file else None,
                    "is_current": False,
                    "created_at": row.created_at,
                }
            )
        if document.merged_revision and document.final_merged_file:
            versions.append(
                {
                    "version": document.merged_revision,
                    "label": f"v{document.merged_revision}",
                    "file": document.final_merged_file.url,
                    "is_current": True,
                    "created_at": document.merged_at,
                }
            )
        return Response({"merged_revision": document.merged_revision, "versions": versions})

    @action(
        detail=True,
        methods=["get"],
        url_path=r"merged-version/(?P<version_num>[0-9]+)/download",
    )
    def download_merged_version(self, request, pk=None, version_num=None):
        document = self.get_object()
        try:
            v = int(version_num)
        except (TypeError, ValueError):
            return Response({"detail": "Invalid version."}, status=status.HTTP_400_BAD_REQUEST)
        if document.merged_revision and v == document.merged_revision:
            return Response(
                {"detail": "Current version is downloaded via download-final."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        row = document.merged_version_history.filter(version=v).first()
        if not row or not row.file or not row.file.name:
            return Response({"detail": "Version not found."}, status=status.HTTP_404_NOT_FOUND)
        inner = row.file.name.rsplit("/", 1)[-1]
        content_type, encoding = mimetypes.guess_type(inner)
        content_type = content_type or "application/octet-stream"
        fh = row.file.open("rb")
        stem = re.sub(r"[^A-Za-z0-9_-]+", "_", document.title or "document").strip("_")[:100] or "document"
        suggested = f"{document.id}_{stem}_v{v}{Path(row.file.name).suffix.lower() or '.docx'}"
        from django.utils.text import get_valid_filename

        suggested = get_valid_filename(suggested)
        response = FileResponse(fh, content_type=content_type, as_attachment=True, filename=suggested)
        response["Content-Disposition"] = (
            f'attachment; filename="{suggested}"; filename*=UTF-8\'\'{quote(suggested)}'
        )
        if encoding:
            response["Content-Encoding"] = encoding
        return response

    @action(detail=True, methods=["post"], url_path="merged-corrected")
    def upload_merged_corrected(self, request, pk=None):
        document = self.get_object()
        f = request.FILES.get("file") or request.FILES.get("processed_file")
        if not f:
            return Response({"detail": "file or processed_file is required."}, status=status.HTTP_400_BAD_REQUEST)
        ext = Path(f.name).suffix.lower() or ".docx"
        if ext not in (".docx", ".doc", ".pdf", ".zip"):
            return Response({"detail": "Unsupported file type."}, status=status.HTTP_400_BAD_REQUEST)
        data = f.read()
        ts = timezone.now().strftime("%Y%m%d%H%M%S")
        storage_name = f"merged_{document.id}_{ts}{ext}"
        actor = request.user if request.user.is_authenticated else None
        finalize_merged_output(document, data, storage_name, actor=actor)
        document.refresh_from_db()
        AuditLog.objects.create(
            action=AuditLog.Action.MERGE_DOC,
            document=document,
            actor=actor,
            metadata={"source": "merged_corrected_upload"},
        )
        return Response(
            {
                "merged_revision": document.merged_revision,
                "merged_file": document.final_merged_file.url if document.final_merged_file else None,
            }
        )

    @action(detail=True, methods=["get"], url_path="resource-processed-bundle")
    def resource_processed_bundle(self, request, pk=None):
        """
        Merge all processed_file artifacts for pages assigned to one resource on this document
        (admin assignment trace 'Download' should use this instead of a single page URL).
        """
        document = self.get_object()
        rid = request.query_params.get("resource_profile_id")
        if not rid:
            return Response({"detail": "resource_profile_id is required"}, status=status.HTTP_400_BAD_REQUEST)
        try:
            profile = ResourceProfile.objects.get(id=int(rid))
        except (ValueError, ResourceProfile.DoesNotExist):
            return Response({"detail": "Invalid resource_profile_id"}, status=status.HTTP_404_NOT_FOUND)

        pages = [
            p
            for p in DocumentPage.objects.filter(document=document, assigned_to=profile).order_by("page_number")
            if p.processed_file and p.processed_file.name
        ]
        if not pages:
            return Response({"detail": "No processed files for this resource on this document"}, status=status.HTTP_404_NOT_FOUND)

        stem = _bundle_file_stem(document.title)
        all_pdf = all((p.processed_file.name or "").lower().endswith(".pdf") for p in pages)
        if all_pdf:
            writer = PdfWriter()
            for page in pages:
                with page.processed_file.open("rb") as stream:
                    reader = PdfReader(stream)
                    for pdf_page in reader.pages:
                        writer.add_page(pdf_page)
            output = io.BytesIO()
            writer.write(output)
            out_name = f"{document.id}_{profile.id}_{stem}_processed_pages_{len(pages)}.pdf"
            response = HttpResponse(output.getvalue(), content_type="application/pdf")
            response["Content-Disposition"] = f'attachment; filename="{out_name}"'
            return response

        # Non-PDF case: if every page points to identical bundled content, return one file.
        unique_payloads: dict[str, tuple[bytes, str]] = {}
        for page in pages:
            with page.processed_file.open("rb") as stream:
                payload = stream.read()
            digest = hashlib.sha256(payload).hexdigest()
            ext = (page.processed_file.name.rsplit(".", 1)[-1] or "bin").lower()
            unique_payloads[digest] = (payload, ext)

        if len(unique_payloads) == 1:
            payload, ext = next(iter(unique_payloads.values()))
            out_name = f"{document.id}_{profile.id}_{stem}_processed_pages_{len(pages)}.{ext}"
            response = HttpResponse(payload, content_type="application/octet-stream")
            response["Content-Disposition"] = f'attachment; filename="{out_name}"'
            return response

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for page in pages:
                with page.processed_file.open("rb") as stream:
                    ext = (page.processed_file.name.rsplit(".", 1)[-1] or "bin").lower()
                    zf.writestr(f"page_{page.page_number}.{ext}", stream.read())
        out_name = f"{document.id}_{profile.id}_{stem}_processed_pages_{len(pages)}.zip"
        response = HttpResponse(buf.getvalue(), content_type="application/zip")
        response["Content-Disposition"] = f'attachment; filename="{out_name}"'
        return response


class DocumentPageViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = DocumentPage.objects.select_related("document", "assigned_to__user").all()
    serializer_class = DocumentPageSerializer
    permission_classes = [IsStaffAdmin]


class ResourceWorkViewSet(
    mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    serializer_class = DocumentPageSerializer
    permission_classes = [IsResourceUser]

    def get_queryset(self):
        scope = self.request.query_params.get("scope", "active")
        qs = DocumentPage.objects.filter(assigned_to=self.request.user.resource_profile).select_related("document")
        if scope == "history":
            return qs.filter(status=DocumentPage.Status.COMPLETED).order_by("-submitted_at", "-updated_at")
        return qs.filter(status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS]).order_by(
            "-assigned_at", "document_id", "page_number"
        )

    @action(detail=True, methods=["post"])
    def download(self, request, pk=None):
        page = self.get_queryset().get(id=pk)
        mark_download_started(page, actor_id=request.user.id)
        return Response({"status": "in_progress"})

    @action(detail=True, methods=["post"], parser_classes=[MultiPartParser, FormParser])
    def submit(self, request, pk=None):
        page = self.get_queryset().get(id=pk)
        processed_file = request.FILES.get("processed_file")
        if not processed_file:
            return Response({"detail": "processed_file is required"}, status=400)
        _, err = _validate_processed_upload(page.document, processed_file.name)
        if err:
            return Response({"detail": err}, status=400)

        page.processed_file = processed_file
        page.status = DocumentPage.Status.COMPLETED
        page.submitted_at = timezone.now()
        page.save(update_fields=["processed_file", "status", "submitted_at", "updated_at"])
        update_document_status(page.document)
        assign_pages_task.delay()
        return Response({"status": "completed"})


@api_view(["PATCH", "DELETE"])
@permission_classes([IsStaffAdmin])
def admin_user_detail(request, pk: int):
    user = get_object_or_404(User, pk=pk)

    if request.method == "DELETE":
        user.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)

    data = request.data
    username = data.get("username")
    email = data.get("email")
    password = data.get("password")
    is_active = data.get("is_active")
    profile_payload = data.get("resource_profile") or {}

    if username is not None:
        user.username = username
    if email is not None:
        user.email = email
    if is_active is not None:
        user.is_active = bool(is_active)
    if password:
        user.set_password(password)
    user.save()

    profile = getattr(user, "resource_profile", None)
    capacity_changed = False
    if profile:
        max_cap = profile_payload.get("max_page_capacity")
        if max_cap is None:
            max_cap = profile_payload.get("max_capacity")
        if max_cap is not None:
            new_cap = int(max_cap)
            capacity_changed = profile.max_page_capacity != new_cap
            profile.max_page_capacity = new_cap
            profile.save(update_fields=["max_page_capacity", "updated_at"])

    # Re-run queue assignment when capacity changes and resource can accept work.
    # Admin capacity edits should make the resource immediately eligible for assignment.
    if profile and capacity_changed and (not profile.is_on_break):
        if profile.is_active_session:
            profile.last_seen_at = timezone.now()
            profile.save(update_fields=["last_seen_at", "updated_at"])
        if profile.remaining_capacity > 0:
            assign_pages_task.delay()

    return Response(
        {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "is_active": user.is_active,
        }
    )


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def heartbeat(request):
    profile = getattr(request.user, "resource_profile", None)
    if profile:
        profile.last_seen_at = timezone.now()
        profile.is_active_session = True
        profile.save(update_fields=["last_seen_at", "is_active_session", "updated_at"])
        if (not profile.is_on_break) and profile.remaining_capacity > 0:
            assign_pages_task.delay()
    return Response({"ok": True})


@api_view(["POST"])
@permission_classes([IsResourceUser])
def resource_break_toggle(request):
    profile = request.user.resource_profile
    enabled = request.data.get("enabled")
    if isinstance(enabled, str):
        enabled = enabled.strip().lower() in {"1", "true", "yes", "on"}
    profile.set_break(bool(enabled))
    # Preserve existing assignments; only pause NEW auto-assignment while on break.
    if not profile.is_on_break and profile.remaining_capacity > 0:
        assign_pages_task.delay()
    return Response(
        {
            "id": profile.id,
            "is_on_break": profile.is_on_break,
            "break_started_at": profile.break_started_at,
            "break_ended_at": profile.break_ended_at,
            "total_break_seconds": profile.total_break_seconds,
        }
    )


@api_view(["GET"])
@permission_classes([IsResourceUser])
def resource_work_bundles(request):
    pages = (
        DocumentPage.objects.filter(
            assigned_to=request.user.resource_profile,
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS],
        )
        .select_related("document")
        .order_by("document_id", "page_number")
    )
    rows = []
    current = None
    for page in pages:
        if (
            current is None
            or current["document_id"] != page.document_id
            or current["page_numbers"][-1] + 1 != page.page_number
        ):
            if current is not None:
                rows.append(current)
            current = {
                "document_id": page.document_id,
                "document_title": page.document.title,
                "document_job_name": (page.document.external_job_name or "").strip(),
                "document_total_pages": page.document.total_pages,
                "resource_id": request.user.resource_profile.id,
                "status": page.status,
                "page_numbers": [],
                "pages_assigned": 0,
            }
        current["page_numbers"].append(page.page_number)
        current["pages_assigned"] += 1
        if page.status == DocumentPage.Status.IN_PROGRESS:
            current["status"] = DocumentPage.Status.IN_PROGRESS
    if current is not None:
        rows.append(current)
    for row in rows:
        stem = _bundle_file_stem(row["document_title"])
        did = row["document_id"]
        rid = row["resource_id"]
        bid = _bundle_id_for_pages(did, row["page_numbers"])
        rng = _page_range_suffix(row["page_numbers"])
        row["bundle_id"] = bid
        row["suggested_download_basename"] = f"{rid}_{did}_{stem}_{bid}_{rng}"
        row["suggested_upload_basename"] = f"{rid}_{did}_{stem}_{bid}_{rng}"
    return Response(rows)


@api_view(["GET"])
@permission_classes([IsResourceUser])
def resource_history_bundles(request):
    pages = (
        DocumentPage.objects.filter(
            assigned_to=request.user.resource_profile,
            status=DocumentPage.Status.COMPLETED,
        )
        .select_related("document")
        .order_by("-submitted_at", "document_id", "page_number")
    )

    search = (request.query_params.get("search") or "").strip()
    if search:
        pages = pages.filter(document__title__icontains=search)

    status_filter = (request.query_params.get("status") or "ALL").upper()
    if status_filter not in ("ALL", "COMPLETED"):
        return Response({"detail": "Invalid status filter."}, status=400)

    date_filter = (request.query_params.get("date") or "").strip()
    if date_filter:
        try:
            selected = date.fromisoformat(date_filter)
            pages = pages.filter(submitted_at__date=selected)
        except ValueError:
            return Response({"detail": "Invalid date format. Use YYYY-MM-DD."}, status=400)

    grouped = {}
    for page in pages:
        key = page.document_id
        row = grouped.get(key)
        if row is None:
            row = {
                "document_id": page.document_id,
                "document_title": page.document.title,
                "document_job_name": (page.document.external_job_name or "").strip(),
                "document_total_pages": page.document.total_pages,
                "status": "COMPLETED",
                "page_numbers": [],
                "pages_completed": 0,
                "last_completed_at": page.submitted_at,
            }
            grouped[key] = row
        row["page_numbers"].append(page.page_number)
        row["pages_completed"] += 1
        if page.submitted_at and (row["last_completed_at"] is None or page.submitted_at > row["last_completed_at"]):
            row["last_completed_at"] = page.submitted_at

    rows = list(grouped.values())
    rows.sort(key=lambda r: (r["last_completed_at"] is not None, r["last_completed_at"]), reverse=True)

    paginator = PageNumberPagination()
    paginator.page_size = 10
    paginator.page_size_query_param = "page_size"
    page = paginator.paginate_queryset(rows, request)
    return paginator.get_paginated_response(page)


@api_view(["GET"])
@permission_classes([IsResourceUser])
def resource_bundle_download(request, document_id: int):
    qs = DocumentPage.objects.filter(
            document_id=document_id,
            assigned_to=request.user.resource_profile,
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS],
    )
    bundle_id = (request.query_params.get("bundle_id") or "").strip()
    if bundle_id:
        start, end = _bundle_range_from_id(bundle_id, document_id)
        if start is None:
            return Response({"detail": "Invalid bundle_id"}, status=400)
        qs = qs.filter(page_number__gte=start, page_number__lte=end)
    pages = list(qs.order_by("page_number"))
    if not pages:
        return Response({"detail": "No assigned pages for this document"}, status=404)

    for page in pages:
        mark_download_started(page, actor_id=request.user.id)

    profile = request.user.resource_profile
    stem = _bundle_file_stem(pages[0].document.title)
    bundle_id = bundle_id or _bundle_id_for_pages(document_id, [p.page_number for p in pages])
    range_suffix = _page_range_suffix([p.page_number for p in pages])
    all_pdf = all((page.split_file and page.split_file.name.lower().endswith(".pdf")) for page in pages)

    if all_pdf:
        writer = PdfWriter()
        for page in pages:
            with page.split_file.open("rb") as stream:
                reader = PdfReader(stream)
                for pdf_page in reader.pages:
                    writer.add_page(pdf_page)
        output = io.BytesIO()
        writer.write(output)
        out_name = f"{profile.id}_{document_id}_{stem}_{bundle_id}_{range_suffix}.pdf"
        response = HttpResponse(output.getvalue(), content_type="application/pdf")
        response["Content-Disposition"] = f'attachment; filename="{out_name}"'
        return response

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        for page in pages:
            if not page.split_file:
                continue
            with page.split_file.open("rb") as stream:
                ext = page.split_file.name.rsplit(".", 1)[-1]
                zf.writestr(f"page_{page.page_number}.{ext}", stream.read())
    out_name = f"{profile.id}_{document_id}_{stem}_{bundle_id}_{range_suffix}.zip"
    response = HttpResponse(output.getvalue(), content_type="application/zip")
    response["Content-Disposition"] = f'attachment; filename="{out_name}"'
    return response


@api_view(["POST"])
@permission_classes([IsResourceUser])
@transaction.atomic
def resource_bundle_submit(request, document_id: int):
    uploaded = request.FILES.get("processed_file")
    if not uploaded:
        return Response({"detail": "processed_file is required"}, status=400)

    qs = (
        DocumentPage.objects.select_for_update().filter(
            document_id=document_id,
            assigned_to=request.user.resource_profile,
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS],
        )
    )
    bundle_id = (request.data.get("bundle_id") or request.query_params.get("bundle_id") or "").strip()
    if bundle_id:
        start, end = _bundle_range_from_id(bundle_id, document_id)
        if start is None:
            return Response({"detail": "Invalid bundle_id"}, status=400)
        qs = qs.filter(page_number__gte=start, page_number__lte=end)
    pages = list(qs.order_by("page_number"))
    if not pages:
        return Response({"detail": "No assigned pages for this document"}, status=404)

    ext, err = _validate_processed_upload(pages[0].document, uploaded.name)
    if err:
        return Response({"detail": err}, status=400)
    payload = uploaded.read()
    range_suffix = _page_range_suffix([p.page_number for p in pages])
    # Flexible handling: any uploaded page count marks this assigned split complete.
    for page in pages:
        page.processed_file.save(
            f"{document_id}_processed_pages_{range_suffix}_page_{page.page_number}.{ext}",
            ContentFile(payload),
            save=False,
        )
        page.status = DocumentPage.Status.COMPLETED
        page.submitted_at = timezone.now()
        page.save(update_fields=["processed_file", "status", "submitted_at", "updated_at"])

    update_document_status(pages[0].document)
    assign_pages_task.delay()
    return Response({"status": "completed", "pages_completed": len(pages)})


def _bundle_file_stem(document_title: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", document_title or "document").strip("_")
    return cleaned or "document"


def _page_range_suffix(page_numbers: list[int]) -> str:
    if not page_numbers:
        return "0-0"
    return f"{min(page_numbers)}-{max(page_numbers)}"


def _bundle_id_for_pages(document_id: int, page_numbers: list[int]) -> str:
    if not page_numbers:
        return f"B{document_id}P0-0"
    return f"B{document_id}P{min(page_numbers)}-{max(page_numbers)}"


def _bundle_range_from_id(bundle_id: str, expected_document_id: int) -> tuple[int, int] | tuple[None, None]:
    if not bundle_id:
        return None, None
    normalized = bundle_id.strip()
    # Ignore Windows duplicate suffixes like " (1)" appended before extension.
    normalized = re.sub(r"\s*\(\d+\)\s*$", "", normalized)

    # Canonical format: B<doc>P<start>-<end> (preferred).
    m = re.match(r"^B(?P<doc>\d+)P(?P<start>\d+)-(?P<end>\d+)$", normalized, re.IGNORECASE)
    if m:
        try:
            doc = int(m.group("doc"))
            start = int(m.group("start"))
            end = int(m.group("end"))
        except Exception:
            return None, None
        if doc != int(expected_document_id):
            return None, None
        return min(start, end), max(start, end)

    # Backward-compatible formats seen from tray uploads:
    # 1) P<start>-<end>
    # 2) <start>-<end>
    m = re.match(r"^P?(?P<start>\d+)-(?P<end>\d+)$", normalized, re.IGNORECASE)
    if not m:
        return None, None
    try:
        start = int(m.group("start"))
        end = int(m.group("end"))
    except Exception:
        return None, None
    return min(start, end), max(start, end)


@api_view(["GET"])
@permission_classes([IsResourceUser])
def resource_tray_package(request):
    """
    Build a per-resource tray package zip:
    - DocProResourceTray.exe
    - config.json (resource_id embedded)
    - Install_or_Update_Tray.bat (kills old tray then starts new)
    """
    profile = request.user.resource_profile
    base_dir = Path(getattr(settings, "BASE_DIR"))
    dist_dir = base_dir / "automation_client" / "dist"
    exe_path = dist_dir / "DocProResourceTray.exe"
    if not exe_path.exists():
        return Response({"detail": "Tray EXE is not available on server. Build and deploy it first."}, status=404)

    base_url = request.build_absolute_uri("/").rstrip("/")
    cfg = {
        "base_url": base_url,
        "download_folder": r"C:\DocPro\downloads",
        "upload_folder": r"C:\DocPro\upload",
        "resource_name": "",
        "resource_id": int(profile.id),
        "isolate_user_folders": True,
        "poll_seconds": 5,
        "watch_seconds": 2,
        "abbyy_exe_path": "",
        "process_triggered_open_only": True,
        "admin_automation_enabled": False,
        "admin_automation_key": "",
        "merged_download_folder": r"C:\DocPro\merged",
        "open_merged_in_word": True,
    }
    cfg_bytes = (json.dumps(cfg, indent=2) + "\n").encode("utf-8")
    launcher = (
        "@echo off\r\n"
        "setlocal\r\n"
        "for /f \"skip=1 tokens=2 delims=,\" %%P in ('tasklist /fo csv /nh /fi \"imagename eq DocProResourceTray.exe\"') do taskkill /pid %%~P /f >nul 2>nul\r\n"
        "timeout /t 1 /nobreak >nul\r\n"
        "start \"\" \"%~dp0DocProResourceTray.exe\"\r\n"
        "exit /b 0\r\n"
    ).encode("utf-8")

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(exe_path, arcname="DocProResourceTray.exe")
        zf.writestr("config.json", cfg_bytes)
        zf.writestr("Install_or_Update_Tray.bat", launcher)
    out_name = f"DocPro_Tray_Resource_{profile.id}.zip"
    response = HttpResponse(output.getvalue(), content_type="application/zip")
    response["Content-Disposition"] = f'attachment; filename="{out_name}"'
    return response


@api_view(["GET"])
@authentication_classes([])
@permission_classes([AllowAny])
def automation_jobs(request):
    resource_id = request.query_params.get("resource_id")
    if not resource_id:
        return Response({"detail": "resource_id is required"}, status=400)
    try:
        profile = ResourceProfile.objects.get(id=int(resource_id))
    except (ValueError, ResourceProfile.DoesNotExist):
        return Response({"detail": "Invalid resource_id"}, status=404)

    pages = (
        DocumentPage.objects.filter(
            assigned_to=profile,
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS],
        )
        .select_related("document")
        .order_by("document_id", "page_number")
    )
    jobs = []
    current = None
    for page in pages:
        if (
            current is None
            or current["job_id"] != page.document_id
            or current["page_numbers"][-1] + 1 != page.page_number
        ):
            if current is not None:
                jobs.append(current)
            current = {
                "resource_id": profile.id,
                "job_id": page.document_id,
                "filename": page.document.title,
                "status": page.status,
                "page_numbers": [],
            }
        current["page_numbers"].append(page.page_number)
        if page.status == DocumentPage.Status.IN_PROGRESS:
            current["status"] = DocumentPage.Status.IN_PROGRESS
    if current is not None:
        jobs.append(current)
    for row in jobs:
        row["bundle_id"] = _bundle_id_for_pages(row["job_id"], row["page_numbers"])
        stem = _bundle_file_stem(row["filename"])
        rng = _page_range_suffix(row["page_numbers"])
        row["suggested_name_pdf"] = f"{row['resource_id']}_{row['job_id']}_{stem}_{row['bundle_id']}_{rng}.pdf"
    return Response(jobs)


@api_view(["GET"])
@authentication_classes([])
@permission_classes([AllowAny])
def automation_job_download(request, job_id: int):
    resource_id = request.query_params.get("resource_id")
    if not resource_id:
        return Response({"detail": "resource_id is required"}, status=400)
    try:
        profile = ResourceProfile.objects.get(id=int(resource_id))
    except (ValueError, ResourceProfile.DoesNotExist):
        return Response({"detail": "Invalid resource_id"}, status=404)

    qs = DocumentPage.objects.filter(
            document_id=job_id,
            assigned_to=profile,
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS],
    )
    bundle_id = (request.query_params.get("bundle_id") or "").strip()
    if bundle_id:
        start, end = _bundle_range_from_id(bundle_id, job_id)
        if start is None:
            return Response({"detail": "Invalid bundle_id"}, status=400)
        qs = qs.filter(page_number__gte=start, page_number__lte=end)
    pages = list(qs.order_by("page_number"))
    if not pages:
        return Response({"detail": "No assigned pages for this job"}, status=404)

    for page in pages:
        mark_download_started(page, actor_id=profile.user_id)

    all_pdf = all((page.split_file and page.split_file.name.lower().endswith(".pdf")) for page in pages)
    stem = _bundle_file_stem(pages[0].document.title)
    bundle_id = bundle_id or _bundle_id_for_pages(job_id, [p.page_number for p in pages])
    range_suffix = _page_range_suffix([p.page_number for p in pages])
    if all_pdf:
        writer = PdfWriter()
        for page in pages:
            with page.split_file.open("rb") as stream:
                reader = PdfReader(stream)
                for pdf_page in reader.pages:
                    writer.add_page(pdf_page)
        output = io.BytesIO()
        writer.write(output)
        out_name = f"{profile.id}_{job_id}_{stem}_{bundle_id}_{range_suffix}.pdf"
        response = HttpResponse(output.getvalue(), content_type="application/pdf")
        response["Content-Disposition"] = f'attachment; filename="{out_name}"'
        return response

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        for page in pages:
            if not page.split_file:
                continue
            with page.split_file.open("rb") as stream:
                ext = page.split_file.name.rsplit(".", 1)[-1]
                zf.writestr(f"page_{page.page_number}.{ext}", stream.read())
    out_name = f"{profile.id}_{job_id}_{stem}_{bundle_id}_{range_suffix}.zip"
    response = HttpResponse(output.getvalue(), content_type="application/zip")
    response["Content-Disposition"] = f'attachment; filename="{out_name}"'
    return response


@api_view(["POST"])
@authentication_classes([])
@permission_classes([AllowAny])
@transaction.atomic
def automation_job_submit(request, job_id: int):
    resource_id = request.data.get("resource_id") or request.query_params.get("resource_id")
    if not resource_id:
        return Response({"detail": "resource_id is required"}, status=400)
    try:
        profile = ResourceProfile.objects.get(id=int(resource_id))
    except (ValueError, ResourceProfile.DoesNotExist):
        return Response({"detail": "Invalid resource_id"}, status=404)

    uploaded = request.FILES.get("processed_file")
    if not uploaded:
        return Response({"detail": "processed_file is required"}, status=400)

    qs = (
        DocumentPage.objects.select_for_update().filter(
            document_id=job_id,
            assigned_to=profile,
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS],
        )
    )
    bundle_id = (request.data.get("bundle_id") or request.query_params.get("bundle_id") or "").strip()
    if bundle_id:
        start, end = _bundle_range_from_id(bundle_id, job_id)
        if start is None:
            return Response({"detail": "Invalid bundle_id"}, status=400)
        qs = qs.filter(page_number__gte=start, page_number__lte=end)
    pages = list(qs.order_by("page_number"))
    if not pages:
        return Response({"detail": "No assigned pages for this job"}, status=404)

    filename = uploaded.name.lower()
    is_pdf = filename.endswith(".pdf")
    is_word = filename.endswith(".docx") or filename.endswith(".doc")
    if not (is_pdf or is_word):
        return Response({"detail": "Only PDF, DOC, or DOCX files are allowed."}, status=400)

    payload = uploaded.read()
    ext = "pdf" if is_pdf else ("docx" if filename.endswith(".docx") else "doc")
    range_suffix = _page_range_suffix([p.page_number for p in pages])
    for page in pages:
        page.processed_file.save(
            f"{job_id}_processed_pages_{range_suffix}_page_{page.page_number}.{ext}",
            ContentFile(payload),
            save=False,
        )
        page.status = DocumentPage.Status.COMPLETED
        page.submitted_at = timezone.now()
        page.save(update_fields=["processed_file", "status", "submitted_at", "updated_at"])

    update_document_status(pages[0].document)
    assign_pages_task.delay()
    return Response({"status": "completed", "pages_completed": len(pages)})
