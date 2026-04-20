from django.contrib.auth import views as auth_views
from django.contrib.auth.models import User
from django.db.models import Count, F, Q
from django.core.paginator import Paginator
from django.shortcuts import redirect, render
from django.contrib.auth.decorators import login_required, user_passes_test
from django.utils import timezone
from datetime import timedelta
from .models import Document, DocumentPage, ResourceProfile
from .tasks import assign_pages_task

ONLINE_TTL_SECONDS = 60


def _is_profile_online(profile: ResourceProfile | None) -> bool:
    if not profile or not profile.is_active_session or not profile.last_seen_at:
        return False
    return profile.last_seen_at >= timezone.now() - timedelta(seconds=ONLINE_TTL_SECONDS)


def _mark_stale_sessions_offline() -> None:
    cutoff = timezone.now() - timedelta(seconds=ONLINE_TTL_SECONDS)
    ResourceProfile.objects.filter(is_active_session=True).filter(
        Q(last_seen_at__isnull=True) | Q(last_seen_at__lt=cutoff)
    ).update(is_active_session=False, updated_at=timezone.now())


def _apply_user_filters(request, base_qs, is_resource: bool):
    search = (request.GET.get("search") or "").strip()
    status = (request.GET.get("status") or "ALL").strip().upper()
    sort = (request.GET.get("sort") or "az").strip().lower()
    page_number = request.GET.get("page") or 1

    qs = base_qs
    if search:
        qs = qs.filter(Q(username__icontains=search) | Q(email__icontains=search))

    if status == "ACTIVE":
        qs = qs.filter(is_active=True)
    elif status == "INACTIVE":
        qs = qs.filter(is_active=False)
    elif status == "ONLINE":
        cutoff = timezone.now() - timedelta(seconds=ONLINE_TTL_SECONDS)
        qs = qs.filter(
            resource_profile__is_active_session=True,
            resource_profile__last_seen_at__gte=cutoff,
        )
    elif status == "OFFLINE":
        cutoff = timezone.now() - timedelta(seconds=ONLINE_TTL_SECONDS)
        qs = qs.exclude(
            resource_profile__is_active_session=True,
            resource_profile__last_seen_at__gte=cutoff,
        )
    elif is_resource and status == "FULL":
        qs = qs.annotate(
            active_load=Count(
                "resource_profile__assigned_pages",
                filter=Q(
                    resource_profile__assigned_pages__status__in=[
                        DocumentPage.Status.ASSIGNED,
                        DocumentPage.Status.IN_PROGRESS,
                    ]
                ),
            )
        ).filter(active_load__gte=F("resource_profile__max_page_capacity"))
    elif is_resource and status == "FREE":
        qs = qs.annotate(
            active_load=Count(
                "resource_profile__assigned_pages",
                filter=Q(
                    resource_profile__assigned_pages__status__in=[
                        DocumentPage.Status.ASSIGNED,
                        DocumentPage.Status.IN_PROGRESS,
                    ]
                ),
            )
        ).filter(active_load__lt=F("resource_profile__max_page_capacity"))

    if sort == "za":
        qs = qs.order_by("-username")
    else:
        qs = qs.order_by("username")

    paginator = Paginator(qs, 10)
    page_obj = paginator.get_page(page_number)
    return page_obj, search, status, sort, paginator


class LoginView(auth_views.LoginView):
    template_name = "auth/login.html"

    def form_valid(self, form):
        response = super().form_valid(form)
        profile = getattr(self.request.user, "resource_profile", None)
        if profile:
            profile.is_active_session = True
            profile.last_seen_at = timezone.now()
            profile.save(update_fields=["is_active_session", "last_seen_at", "updated_at"])
            assign_pages_task.delay()
        return response

    def get_success_url(self):
        if self.request.user.is_staff:
            return self.get_redirect_url() or "/admin/dashboard/"
        return self.get_redirect_url() or "/resource/fetch/"


class LogoutView(auth_views.LogoutView):
    next_page = "/login/"

    def dispatch(self, request, *args, **kwargs):
        profile = getattr(request.user, "resource_profile", None) if request.user.is_authenticated else None
        if profile:
            profile.is_active_session = False
            profile.save(update_fields=["is_active_session", "updated_at"])
        return super().dispatch(request, *args, **kwargs)


def home(request):
    if request.user.is_authenticated:
        if request.user.is_staff:
            return redirect("admin_panel:dashboard")
        return redirect("resource_fetch")
    return redirect("login")


def _is_staff_admin(user):
    return user.is_authenticated and user.is_staff


@login_required
@user_passes_test(_is_staff_admin)
def dashboard(request):
    _mark_stale_sessions_offline()
    context = {
        "total_docs": Document.objects.count(),
        "total_resources": ResourceProfile.objects.count(),
        "active_resources": ResourceProfile.objects.filter(is_active_session=True).count(),
        "processing_docs": Document.objects.filter(status=Document.Status.IN_PROGRESS).count(),
        "pending_reviews": DocumentPage.objects.filter(status=DocumentPage.Status.PENDING_APPROVAL).count(),
        "assigned_pages_count": DocumentPage.objects.filter(status=DocumentPage.Status.ASSIGNED).count(),
        "unassigned_pages_count": DocumentPage.objects.filter(status=DocumentPage.Status.NOT_ASSIGNED).count(),
        "unassigned_docs_count": (
            Document.objects.annotate(
                unassigned_count=Count("pages", filter=Q(pages__status=DocumentPage.Status.NOT_ASSIGNED))
            )
            .filter(unassigned_count__gt=0)
            .count()
        ),
    }
    return render(request, "admin/dashboard.html", context)


@login_required
@user_passes_test(_is_staff_admin)
def upload_page(request):
    return render(request, "admin/upload.html")


@login_required
@user_passes_test(_is_staff_admin)
def document_list(request):
    return render(request, "admin/documents.html")


@login_required
@user_passes_test(_is_staff_admin)
def resource_list(request):
    _mark_stale_sessions_offline()
    base_qs = (
        User.objects.filter(resource_profile__isnull=False)
        .select_related("resource_profile")
    )
    page_obj, current_search, current_status, current_sort, paginator = _apply_user_filters(
        request,
        base_qs,
        is_resource=True,
    )
    users = list(page_obj.object_list)
    for user in users:
        user.role = "RESOURCE"
        user.is_online = _is_profile_online(user.resource_profile)
        user.is_working = user.resource_profile.current_load > 0
        user.resource_profile.max_capacity = user.resource_profile.max_page_capacity
        user.resource_profile.active_load = user.resource_profile.current_load
        user.resource_profile.manual_upload_enabled = bool(user.resource_profile.manual_upload_enabled)
    return render(
        request,
        "admin/resource_list.html",
        {
            "users": users,
            "page_obj": page_obj,
            "paginator": paginator,
            "current_search": current_search,
            "current_status": current_status,
            "current_sort": current_sort,
        },
    )


@login_required
@user_passes_test(_is_staff_admin)
def create_resource(request):
    return render(request, "admin/create_resource.html")


@login_required
@user_passes_test(_is_staff_admin)
def client_list(request):
    _mark_stale_sessions_offline()
    base_qs = (
        User.objects.filter(is_staff=False, resource_profile__isnull=True)
        .order_by("username")
    )
    page_obj, current_search, current_status, current_sort, paginator = _apply_user_filters(
        request,
        base_qs,
        is_resource=False,
    )
    users = list(page_obj.object_list)
    for user in users:
        user.role = "CLIENT"
        user.is_online = False
        user.is_working = False
    return render(
        request,
        "admin/client_list.html",
        {
            "users": users,
            "page_obj": page_obj,
            "paginator": paginator,
            "current_search": current_search,
            "current_status": current_status,
            "current_sort": current_sort,
        },
    )


@login_required
@user_passes_test(_is_staff_admin)
def create_client(request):
    return render(request, "admin/create_client.html")


@login_required
@user_passes_test(_is_staff_admin)
def create_admin(request):
    return render(request, "admin/create_admin.html")


@login_required
def resource_fetch(request):
    return render(request, "resource/fetch_work.html")


@login_required
def resource_history(request):
    return render(request, "resource/history.html")


@login_required
def resource_past_work_update(request):
    return render(request, "resource/history.html")


@login_required
def resource_profile(request):
    return render(request, "resource/profile.html")


@login_required
def resource_submit(request):
    return render(request, "resource/submit_work.html")

