from django.contrib.auth.models import User
from datetime import timedelta
from django.utils import timezone
from rest_framework import serializers
from .models import AssignmentQueue, AuditLog, Document, DocumentPage, ResourceProfile
from .services import detect_file_type, get_total_pages

ONLINE_TTL_SECONDS = 60


class ResourceCreateSerializer(serializers.ModelSerializer):
    username = serializers.CharField(write_only=True)
    email = serializers.EmailField(write_only=True)
    password = serializers.CharField(write_only=True, min_length=8)

    class Meta:
        model = ResourceProfile
        fields = ("id", "username", "email", "password", "max_page_capacity", "is_active_session")
        read_only_fields = ("is_active_session",)

    def create(self, validated_data):
        username = validated_data.pop("username")
        email = validated_data.pop("email")
        password = validated_data.pop("password")
        user = User.objects.create_user(username=username, email=email, password=password)
        return ResourceProfile.objects.create(user=user, **validated_data)


class ResourceSerializer(serializers.ModelSerializer):
    username = serializers.CharField(source="user.username", read_only=True)
    email = serializers.EmailField(source="user.email", read_only=True)
    remaining_capacity = serializers.IntegerField(read_only=True)
    is_active_session = serializers.SerializerMethodField()
    current_break_seconds = serializers.SerializerMethodField()

    class Meta:
        model = ResourceProfile
        fields = (
            "id",
            "username",
            "email",
            "max_page_capacity",
            "manual_upload_enabled",
            "word_split_layout_ratio",
            "is_active_session",
            "is_on_break",
            "break_started_at",
            "break_ended_at",
            "total_break_seconds",
            "current_break_seconds",
            "last_seen_at",
            "remaining_capacity",
        )

    def get_is_active_session(self, obj: ResourceProfile) -> bool:
        if not obj.is_active_session or not obj.last_seen_at:
            return False
        return obj.last_seen_at >= timezone.now() - timedelta(seconds=ONLINE_TTL_SECONDS)

    def get_current_break_seconds(self, obj: ResourceProfile) -> int:
        if not obj.is_on_break or not obj.break_started_at:
            return int(obj.total_break_seconds or 0)
        live = max(int((timezone.now() - obj.break_started_at).total_seconds()), 0)
        return int(obj.total_break_seconds or 0) + live


class DocumentSerializer(serializers.ModelSerializer):
    created_at = serializers.DateTimeField(source="uploaded_at", read_only=True)
    assigned_resources = serializers.SerializerMethodField()
    merged_versions = serializers.SerializerMethodField()
    available_resources = serializers.SerializerMethodField()
    overall_processing_seconds = serializers.SerializerMethodField()

    class Meta:
        model = Document
        fields = (
            "id",
            "title",
            "original_file",
            "file_type",
            "total_pages",
            "status",
            "uploaded_by",
            "uploaded_at",
            "created_at",
            "final_merged_file",
            "merged_at",
            "merged_revision",
            "is_on_hold",
            "is_urgent",
            "prioritized_at",
            "external_job_id",
            "external_job_name",
            "overall_processing_seconds",
            "merged_versions",
            "available_resources",
            "assigned_resources",
        )
        read_only_fields = (
            "file_type",
            "total_pages",
            "status",
            "uploaded_by",
            "uploaded_at",
            "final_merged_file",
            "merged_at",
            "merged_revision",
            "is_on_hold",
            "is_urgent",
            "prioritized_at",
            "external_job_id",
            "external_job_name",
            "overall_processing_seconds",
            "merged_versions",
            "available_resources",
        )
        extra_kwargs = {"title": {"required": False, "allow_blank": True}}

    def create(self, validated_data):
        upload = validated_data["original_file"]
        validated_data["file_type"] = detect_file_type(upload.name)
        if not validated_data.get("title"):
            validated_data["title"] = upload.name.rsplit(".", 1)[0]
        document = super().create(validated_data)
        document.total_pages = get_total_pages(document)
        document.save(update_fields=["total_pages", "updated_at"])
        return document

    def get_merged_versions(self, obj: Document):
        out = []
        for row in obj.merged_version_history.order_by("version"):
            out.append(
                {
                    "version": row.version,
                    "label": f"v{row.version}",
                    "file": row.file.url if row.file else None,
                    "is_current": False,
                }
            )
        if obj.merged_revision and obj.final_merged_file:
            out.append(
                {
                    "version": obj.merged_revision,
                    "label": f"v{obj.merged_revision}",
                    "file": obj.final_merged_file.url,
                    "is_current": True,
                }
            )
        return out

    def get_assigned_resources(self, obj: Document):
        pages = obj.pages.select_related("assigned_to__user").order_by("page_number")
        return [
            {
                "id": p.id,
                "username": p.assigned_to.user.username if p.assigned_to_id else "Unassigned",
                "resource_profile_id": p.assigned_to_id,
                "page_number": p.page_number,
                "status": p.status,
                "status_raw": p.status,
                "completed_at": p.submitted_at,
                "viewed_at": p.download_started_at,
                "download_started_at": p.download_started_at,
                "submitted_at": p.submitted_at,
                "split_completion_seconds": (
                    max(int((p.submitted_at - p.assigned_at).total_seconds()), 0)
                    if p.assigned_at and p.submitted_at
                    else None
                ),
                "processed_file": p.processed_file.url if p.processed_file else None,
                "is_on_hold": p.is_on_hold,
            }
            for p in pages
        ]

    def get_available_resources(self, obj: Document):
        return [
            {"id": r.id, "username": r.user.username, "manual_upload_enabled": r.manual_upload_enabled}
            for r in ResourceProfile.objects.select_related("user").order_by("user__username")
        ]

    def get_overall_processing_seconds(self, obj: Document):
        if not obj.uploaded_at:
            return None
        end = obj.merged_at or timezone.now()
        return max(int((end - obj.uploaded_at).total_seconds()), 0)


class DocumentPageSerializer(serializers.ModelSerializer):
    assigned_to_username = serializers.CharField(source="assigned_to.user.username", read_only=True)
    document_title = serializers.CharField(source="document.title", read_only=True)

    class Meta:
        model = DocumentPage
        fields = (
            "id",
            "document",
            "document_title",
            "page_number",
            "split_file",
            "processed_file",
            "status",
            "is_on_hold",
            "assigned_to",
            "assigned_to_username",
            "assigned_at",
            "download_started_at",
            "submitted_at",
        )
        read_only_fields = ("assigned_at", "download_started_at", "submitted_at")


class AssignmentQueueSerializer(serializers.ModelSerializer):
    class Meta:
        model = AssignmentQueue
        fields = ("id", "page", "reason", "queued_at")


class AuditLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditLog
        fields = ("id", "actor", "action", "document", "page", "metadata", "created_at")
