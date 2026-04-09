from django.contrib import admin
from .models import AssignmentQueue, AuditLog, Document, DocumentPage, ResourceProfile


@admin.register(ResourceProfile)
class ResourceProfileAdmin(admin.ModelAdmin):
    list_display = ("user", "max_page_capacity", "is_active_session", "last_seen_at")
    search_fields = ("user__username", "user__email")


class DocumentPageInline(admin.TabularInline):
    model = DocumentPage
    extra = 0
    fields = ("page_number", "status", "assigned_to", "assigned_at")
    readonly_fields = ("page_number",)


@admin.register(Document)
class DocumentAdmin(admin.ModelAdmin):
    list_display = ("id", "title", "file_type", "total_pages", "status", "uploaded_at")
    search_fields = ("title",)
    list_filter = ("status", "file_type")
    inlines = [DocumentPageInline]


@admin.register(DocumentPage)
class DocumentPageAdmin(admin.ModelAdmin):
    list_display = ("id", "document", "page_number", "status", "assigned_to")
    list_filter = ("status",)
    search_fields = ("document__title",)


@admin.register(AssignmentQueue)
class AssignmentQueueAdmin(admin.ModelAdmin):
    list_display = ("page", "reason", "queued_at")
    list_filter = ("reason",)


@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    list_display = ("action", "actor", "document", "page", "created_at")
    list_filter = ("action",)
