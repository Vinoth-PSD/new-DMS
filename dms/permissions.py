import secrets

from django.conf import settings
from rest_framework.permissions import BasePermission


class IsStaffAdmin(BasePermission):
    def has_permission(self, request, view) -> bool:
        return bool(request.user and request.user.is_authenticated and request.user.is_staff)


class IsResourceUser(BasePermission):
    def has_permission(self, request, view) -> bool:
        user = request.user
        return bool(user and user.is_authenticated and hasattr(user, "resource_profile"))


class IsStaffAdminOrAutomationKey(BasePermission):
    """Staff session, or X-Admin-Automation-Key matching ADMIN_AUTOMATION_KEY (for tray / automation uploads)."""

    def has_permission(self, request, view) -> bool:
        if request.user and request.user.is_authenticated and getattr(request.user, "is_staff", False):
            return True
        expected = (getattr(settings, "ADMIN_AUTOMATION_KEY", None) or "") or ""
        if not expected.strip():
            return False
        got = (request.headers.get("X-Admin-Automation-Key") or "").strip()
        if not got:
            return False
        try:
            return secrets.compare_digest(got.encode("utf-8"), expected.encode("utf-8"))
        except Exception:
            return False
