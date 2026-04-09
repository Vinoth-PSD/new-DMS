from rest_framework.permissions import BasePermission


class IsStaffAdmin(BasePermission):
    def has_permission(self, request, view) -> bool:
        return bool(request.user and request.user.is_authenticated and request.user.is_staff)


class IsResourceUser(BasePermission):
    def has_permission(self, request, view) -> bool:
        user = request.user
        return bool(user and user.is_authenticated and hasattr(user, "resource_profile"))
