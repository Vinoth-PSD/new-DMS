from django.conf import settings
from django.conf.urls.static import static
from django.contrib import admin
from django.urls import include, path
from dms import web_views

urlpatterns = [
    path("", web_views.home, name="home"),
    path("login/", web_views.LoginView.as_view(), name="login"),
    path("logout/", web_views.LogoutView.as_view(), name="logout"),
    path("admin/", include(("dms.web_urls", "admin_panel"), namespace="admin_panel")),
    path("resource/fetch/", web_views.resource_fetch, name="resource_fetch"),
    path("resource/history/", web_views.resource_history, name="resource_history"),
    path("resource/past-work-update/", web_views.resource_past_work_update, name="resource_past_work_update"),
    path("resource/profile/", web_views.resource_profile, name="resource_profile"),
    path("resource/submit/", web_views.resource_submit, name="resource_submit"),
    path("django-admin/", admin.site.urls),
    path("api/", include("dms.urls")),
]

# if settings.DEBUG or getattr(settings, "SERVE_MEDIA", False):
#     urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
