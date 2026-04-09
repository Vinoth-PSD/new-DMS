from django.urls import path
from . import web_views

app_name = "admin_panel"

urlpatterns = [
    path("dashboard/", web_views.dashboard, name="dashboard"),
    path("resources/", web_views.resource_list, name="resource_list"),
    path("resources/new/", web_views.create_resource, name="create_resource"),
    path("upload/", web_views.upload_page, name="upload_page"),
    path("documents/", web_views.document_list, name="document_list"),
    path("clients/", web_views.client_list, name="client_list"),
    path("clients/new/", web_views.create_client, name="create_client"),
    path("admins/new/", web_views.create_admin, name="create_admin"),
]

