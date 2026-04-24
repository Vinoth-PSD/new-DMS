from django.urls import include, path
from rest_framework.routers import DefaultRouter
from .views import (
    AdminDashboardViewSet,
    DocumentPageViewSet,
    DocumentViewSet,
    ResourceViewSet,
    ResourceWorkViewSet,
    admin_user_detail,
    heartbeat,
    resource_work_bundles,
    resource_history_bundles,
    resource_bundle_download,
    resource_bundle_submit,
    resource_tray_package,
    automation_jobs,
    automation_job_download,
    automation_job_submit,
)

router = DefaultRouter()
router.register(r"admin/dashboard", AdminDashboardViewSet, basename="admin-dashboard")
router.register(r"admin/resources", ResourceViewSet, basename="admin-resources")
router.register(r"admin/documents", DocumentViewSet, basename="admin-documents")
router.register(r"admin/pages", DocumentPageViewSet, basename="admin-pages")
router.register(r"resource/work", ResourceWorkViewSet, basename="resource-work")

urlpatterns = [
    path("admin/users/<int:pk>/", admin_user_detail, name="admin-user-detail"),
    path("v1/auth/users/heartbeat/", heartbeat, name="legacy-heartbeat"),
    path("resource/work/bundles/", resource_work_bundles, name="resource-work-bundles"),
    path("resource/work/tray-package/", resource_tray_package, name="resource-tray-package"),
    path("resource/work/history-bundles/", resource_history_bundles, name="resource-history-bundles"),
    path("resource/work/bundles/<int:document_id>/download/", resource_bundle_download, name="resource-bundle-download"),
    path("resource/work/bundles/<int:document_id>/submit/", resource_bundle_submit, name="resource-bundle-submit"),
    path("automation/jobs/", automation_jobs, name="automation-jobs"),
    path("automation/jobs/<int:job_id>/download/", automation_job_download, name="automation-job-download"),
    path("automation/jobs/<int:job_id>/submit/", automation_job_submit, name="automation-job-submit"),
    path("", include(router.urls)),
]
