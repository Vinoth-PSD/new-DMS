from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class ResourceProfile(TimeStampedModel):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="resource_profile"
    )
    max_page_capacity = models.PositiveIntegerField(default=10)
    is_active_session = models.BooleanField(default=False)
    last_seen_at = models.DateTimeField(null=True, blank=True)
    manual_upload_enabled = models.BooleanField(
        default=False,
        help_text="When enabled, admin can manually assign held/reassigned jobs to this resource; auto-assignment skips this resource.",
    )

    def __str__(self) -> str:
        return f"{self.user.username} ({self.max_page_capacity})"

    @property
    def current_load(self) -> int:
        return self.assigned_pages.filter(
            status__in=[DocumentPage.Status.ASSIGNED, DocumentPage.Status.IN_PROGRESS]
        ).count()

    @property
    def remaining_capacity(self) -> int:
        return max(self.max_page_capacity - self.current_load, 0)

    @property
    def is_available(self) -> bool:
        return self.is_active_session and self.remaining_capacity > 0

    def mark_active(self) -> None:
        self.is_active_session = True
        self.last_seen_at = timezone.now()
        self.save(update_fields=["is_active_session", "last_seen_at", "updated_at"])


def validate_document_extension(value) -> None:
    valid = [".pdf", ".docx"]
    if not any(str(value.name).lower().endswith(ext) for ext in valid):
        raise ValidationError("Only PDF and DOCX files are allowed.")


class Document(TimeStampedModel):
    class FileType(models.TextChoices):
        PDF = "PDF", "PDF"
        DOCX = "DOCX", "DOCX"

    class Status(models.TextChoices):
        NOT_ASSIGNED = "NOT_ASSIGNED", "Not Assigned"
        ASSIGNED = "ASSIGNED", "Assigned"
        IN_PROGRESS = "IN_PROGRESS", "In Progress"
        COMPLETED = "COMPLETED", "Completed"
        PENDING_APPROVAL = "PENDING_APPROVAL", "Pending Approval"
        ON_HOLD = "ON_HOLD", "On Hold"

    title = models.CharField(max_length=255)
    original_file = models.FileField(
        upload_to="original/%Y/%m/%d/", validators=[validate_document_extension]
    )
    file_type = models.CharField(max_length=8, choices=FileType.choices)
    total_pages = models.PositiveIntegerField(default=0)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.NOT_ASSIGNED)
    uploaded_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name="uploaded_documents"
    )
    uploaded_at = models.DateTimeField(default=timezone.now)
    final_merged_file = models.FileField(upload_to="final/%Y/%m/%d/", null=True, blank=True)
    merged_at = models.DateTimeField(null=True, blank=True)
    merged_revision = models.PositiveIntegerField(
        default=0,
        help_text="Current merged file version number (v1, v2, …). Increments on each merge or correction upload.",
    )
    is_on_hold = models.BooleanField(default=False)
    is_urgent = models.BooleanField(default=False)
    prioritized_at = models.DateTimeField(null=True, blank=True)

    def __str__(self) -> str:
        return self.title


class MergedFileVersion(TimeStampedModel):
    """Superseded merged outputs (v1, v2, …). Latest merged file stays on Document.final_merged_file."""

    document = models.ForeignKey(Document, on_delete=models.CASCADE, related_name="merged_version_history")
    version = models.PositiveIntegerField()  # 1-based snapshot index when superseded
    file = models.FileField(upload_to="merged_versions/%Y/%m/%d/")
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name="merged_versions_saved"
    )

    class Meta:
        ordering = ("version",)
        unique_together = ("document", "version")

    def __str__(self) -> str:
        return f"{self.document_id} merged v{self.version}"


class DocumentPage(TimeStampedModel):
    class Status(models.TextChoices):
        NOT_ASSIGNED = "NOT_ASSIGNED", "Not Assigned"
        ASSIGNED = "ASSIGNED", "Assigned"
        IN_PROGRESS = "IN_PROGRESS", "In Progress"
        COMPLETED = "COMPLETED", "Completed"
        PENDING_APPROVAL = "PENDING_APPROVAL", "Pending Approval"
        ON_HOLD = "ON_HOLD", "On Hold"
        REASSIGNED = "REASSIGNED", "Reassigned"

    document = models.ForeignKey(Document, on_delete=models.CASCADE, related_name="pages")
    page_number = models.PositiveIntegerField()
    split_file = models.FileField(upload_to="split/%Y/%m/%d/", null=True, blank=True)
    processed_file = models.FileField(upload_to="processed/%Y/%m/%d/", null=True, blank=True)
    status = models.CharField(max_length=20, choices=Status.choices, default=Status.NOT_ASSIGNED)
    assigned_to = models.ForeignKey(
        ResourceProfile, on_delete=models.SET_NULL, null=True, blank=True, related_name="assigned_pages"
    )
    assigned_at = models.DateTimeField(null=True, blank=True)
    download_started_at = models.DateTimeField(null=True, blank=True)
    submitted_at = models.DateTimeField(null=True, blank=True)
    is_on_hold = models.BooleanField(default=False)

    class Meta:
        unique_together = ("document", "page_number")
        ordering = ("document_id", "page_number")

    def __str__(self) -> str:
        return f"{self.document_id}-P{self.page_number}"


class AssignmentQueue(TimeStampedModel):
    class Reason(models.TextChoices):
        NO_CAPACITY = "NO_CAPACITY", "No capacity"
        NO_ACTIVE_RESOURCE = "NO_ACTIVE_RESOURCE", "No active resource"

    page = models.OneToOneField(DocumentPage, on_delete=models.CASCADE, related_name="queue_item")
    reason = models.CharField(max_length=20, choices=Reason.choices)
    queued_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ("queued_at",)


class AuditLog(TimeStampedModel):
    class Action(models.TextChoices):
        UPLOAD_DOC = "UPLOAD_DOC", "Upload document"
        SPLIT_DOC = "SPLIT_DOC", "Split document"
        ASSIGN_PAGE = "ASSIGN_PAGE", "Assign page"
        DOWNLOAD_PAGE = "DOWNLOAD_PAGE", "Download page"
        SUBMIT_PAGE = "SUBMIT_PAGE", "Submit page"
        MERGE_DOC = "MERGE_DOC", "Merge document"
        APPROVE_PAGE = "APPROVE_PAGE", "Approve page"

    actor = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name="audit_logs"
    )
    action = models.CharField(max_length=20, choices=Action.choices)
    document = models.ForeignKey(Document, on_delete=models.SET_NULL, null=True, blank=True, related_name="audit_logs")
    page = models.ForeignKey(DocumentPage, on_delete=models.SET_NULL, null=True, blank=True, related_name="audit_logs")
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ("-created_at",)
