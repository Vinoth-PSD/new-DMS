import io
import mimetypes
import zipfile
import re
import hashlib
from datetime import date
from pathlib import Path
from urllib.parse import quote
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
            qs = qs.filter(Q(title__icontains=search) | Q(original_file__icontains=search))
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
        target_id = request.data.get("resource_profile_id")
        page_ids = request.data.get("page_ids") or []
        if not target_id or not page_ids:
            return Response({"detail": "resource_profile_id and page_ids are required"}, status=400)
        try:
            target = ResourceProfile.objects.get(id=int(target_id))
        except (ResourceProfile.DoesNotExist, ValueError):
            return Response({"detail": "Invalid resource_profile_id"}, status=404)
        pages = list(document.pages.filter(id__in=page_ids).order_by("page_number"))
        now = timezone.now()
        for page in pages:
            if page.status == DocumentPage.Status.COMPLETED:
                continue
            page.assigned_to = target
            page.assigned_at = now
            page.is_on_hold = False
            page.status = DocumentPage.Status.ASSIGNED
            page.save(update_fields=["assigned_to", "assigned_at", "is_on_hold", "status", "updated_at"])
        update_document_status(document)
        return Response({"updated": len(pages), "status": "reassigned"})

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
    if profile:
        max_cap = profile_payload.get("max_page_capacity")
        if max_cap is None:
            max_cap = profile_payload.get("max_capacity")
        if max_cap is not None:
            profile.max_page_capacity = int(max_cap)
            profile.save(update_fields=["max_page_capacity", "updated_at"])

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
        if profile.remaining_capacity > 0:
            assign_pages_task.delay()
    return Response({"ok": True})


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
    grouped = {}
    for page in pages:
        key = page.document_id
        if key not in grouped:
            grouped[key] = {
                "document_id": page.document_id,
                "document_title": page.document.title,
                "resource_id": request.user.resource_profile.id,
                "status": page.status,
                "page_numbers": [],
                "pages_assigned": 0,
            }
        grouped[key]["page_numbers"].append(page.page_number)
        grouped[key]["pages_assigned"] += 1
        if page.status == DocumentPage.Status.IN_PROGRESS:
            grouped[key]["status"] = DocumentPage.Status.IN_PROGRESS
    rows = list(grouped.values())
    for row in rows:
        stem = _bundle_file_stem(row["document_title"])
        did = row["document_id"]
        rid = row["resource_id"]
        rng = _page_range_suffix(row["page_numbers"])
        row["suggested_download_basename"] = f"{rid}_{did}_{stem}_B{did}_{rng}"
        row["suggested_upload_basename"] = f"{rid}_{did}_{stem}_B{did}_{rng}"
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
    pages = list(
        DocumentPage.objects.filter(
            document_id=document_id,
            assigned_to=request.user.resource_profile,
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS],
        ).order_by("page_number")
    )
    if not pages:
        return Response({"detail": "No assigned pages for this document"}, status=404)

    for page in pages:
        mark_download_started(page, actor_id=request.user.id)

    profile = request.user.resource_profile
    stem = _bundle_file_stem(pages[0].document.title)
    bundle_id = f"B{document_id}"
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

    pages = list(
        DocumentPage.objects.select_for_update()
        .filter(
            document_id=document_id,
            assigned_to=request.user.resource_profile,
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS],
        )
        .order_by("page_number")
    )
    if not pages:
        return Response({"detail": "No assigned pages for this document"}, status=404)

    filename = uploaded.name.lower()
    is_pdf = filename.endswith(".pdf")
    is_word = filename.endswith(".docx") or filename.endswith(".doc")
    if not (is_pdf or is_word):
        return Response({"detail": "Only PDF, DOC, or DOCX files are allowed."}, status=400)

    payload = uploaded.read()
    ext = "pdf" if is_pdf else ("docx" if filename.endswith(".docx") else "doc")
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
    grouped = {}
    for page in pages:
        key = page.document_id
        if key not in grouped:
            grouped[key] = {
                "resource_id": profile.id,
                "job_id": page.document_id,
                "bundle_id": f"B{page.document_id}",
                "filename": page.document.title,
                "status": page.status,
                "page_numbers": [],
            }
        grouped[key]["page_numbers"].append(page.page_number)
        if page.status == DocumentPage.Status.IN_PROGRESS:
            grouped[key]["status"] = DocumentPage.Status.IN_PROGRESS

    jobs = []
    for row in grouped.values():
        stem = _bundle_file_stem(row["filename"])
        rng = _page_range_suffix(row["page_numbers"])
        row["suggested_name_pdf"] = f"{row['resource_id']}_{row['job_id']}_{stem}_{row['bundle_id']}_{rng}.pdf"
        jobs.append(row)
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

    pages = list(
        DocumentPage.objects.filter(
            document_id=job_id,
            assigned_to=profile,
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS],
        ).order_by("page_number")
    )
    if not pages:
        return Response({"detail": "No assigned pages for this job"}, status=404)

    for page in pages:
        mark_download_started(page, actor_id=profile.user_id)

    all_pdf = all((page.split_file and page.split_file.name.lower().endswith(".pdf")) for page in pages)
    stem = _bundle_file_stem(pages[0].document.title)
    bundle_id = f"B{job_id}"
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

    pages = list(
        DocumentPage.objects.select_for_update()
        .filter(
            document_id=job_id,
            assigned_to=profile,
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS],
        )
        .order_by("page_number")
    )
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
