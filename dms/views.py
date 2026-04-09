import io
import zipfile
import re
import hashlib
from datetime import date
from django.db.models import Count, Q
from django.utils import timezone
from django.contrib.auth.models import User
from django.shortcuts import get_object_or_404
from django.http import HttpResponse
from django.db import transaction
from django.core.files.base import ContentFile
from rest_framework import mixins, status, viewsets
from rest_framework.decorators import action, api_view, authentication_classes, permission_classes
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from pypdf import PdfReader, PdfWriter
from .models import Document, DocumentPage, ResourceProfile
from .permissions import IsResourceUser, IsStaffAdmin
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


class DocumentViewSet(viewsets.ModelViewSet):
    queryset = Document.objects.all().order_by("-uploaded_at")
    serializer_class = DocumentSerializer
    permission_classes = [IsStaffAdmin]
    parser_classes = [MultiPartParser, FormParser]

    def perform_create(self, serializer):
        document = serializer.save(uploaded_by=self.request.user)
        split_document_task.delay(document.id)

    @action(detail=True, methods=["post"])
    def assign(self, request, pk=None):
        assign_pages_task.delay(document_id=int(pk))
        return Response({"queued": True}, status=status.HTTP_202_ACCEPTED)

    @action(detail=True, methods=["post"])
    def merge(self, request, pk=None):
        result = merge_document_task(document_id=int(pk))
        return Response(result, status=status.HTTP_200_OK if result.get("merged") else status.HTTP_400_BAD_REQUEST)

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
        row["suggested_download_basename"] = f"{rid}_{did}_{stem}_B{did}"
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
        out_name = f"{profile.id}_{document_id}_{stem}_{bundle_id}.pdf"
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
    out_name = f"{profile.id}_{document_id}_{stem}_{bundle_id}.zip"
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

    if is_pdf:
        reader = PdfReader(uploaded)
        if len(reader.pages) != len(pages):
            return Response(
                {
                    "detail": f"Uploaded PDF pages ({len(reader.pages)}) must equal assigned pages ({len(pages)}).",
                },
                status=400,
            )
        for idx, page in enumerate(pages):
            writer = PdfWriter()
            writer.add_page(reader.pages[idx])
            buffer = io.BytesIO()
            writer.write(buffer)
            page.processed_file.save(
                f"{document_id}_processed_page_{page.page_number}.pdf",
                ContentFile(buffer.getvalue()),
                save=False,
            )
            page.status = DocumentPage.Status.COMPLETED
            page.submitted_at = timezone.now()
            page.save(update_fields=["processed_file", "status", "submitted_at", "updated_at"])
    else:
        # Word upload is accepted as a bundled corrected artifact for assigned pages.
        payload = uploaded.read()
        ext = "docx" if filename.endswith(".docx") else "doc"
        for page in pages:
            page.processed_file.save(
                f"{document_id}_processed_page_{page.page_number}.{ext}",
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
        row["suggested_name_pdf"] = f"{row['resource_id']}_{row['job_id']}_{stem}_{row['bundle_id']}.pdf"
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
    if all_pdf:
        writer = PdfWriter()
        for page in pages:
            with page.split_file.open("rb") as stream:
                reader = PdfReader(stream)
                for pdf_page in reader.pages:
                    writer.add_page(pdf_page)
        output = io.BytesIO()
        writer.write(output)
        out_name = f"{profile.id}_{job_id}_{stem}_{bundle_id}.pdf"
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
    out_name = f"{profile.id}_{job_id}_{stem}_{bundle_id}.zip"
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

    if is_pdf:
        reader = PdfReader(uploaded)
        if len(reader.pages) != len(pages):
            return Response(
                {"detail": f"Uploaded PDF pages ({len(reader.pages)}) must equal assigned pages ({len(pages)})."},
                status=400,
            )
        for idx, page in enumerate(pages):
            writer = PdfWriter()
            writer.add_page(reader.pages[idx])
            buffer = io.BytesIO()
            writer.write(buffer)
            page.processed_file.save(
                f"{job_id}_processed_page_{page.page_number}.pdf",
                ContentFile(buffer.getvalue()),
                save=False,
            )
            page.status = DocumentPage.Status.COMPLETED
            page.submitted_at = timezone.now()
            page.save(update_fields=["processed_file", "status", "submitted_at", "updated_at"])
    else:
        payload = uploaded.read()
        ext = "docx" if filename.endswith(".docx") else "doc"
        for page in pages:
            page.processed_file.save(
                f"{job_id}_processed_page_{page.page_number}.{ext}",
                ContentFile(payload),
                save=False,
            )
            page.status = DocumentPage.Status.COMPLETED
            page.submitted_at = timezone.now()
            page.save(update_fields=["processed_file", "status", "submitted_at", "updated_at"])

    update_document_status(pages[0].document)
    assign_pages_task.delay()
    return Response({"status": "completed", "pages_completed": len(pages)})
