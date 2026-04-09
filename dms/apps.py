from django.apps import AppConfig


class DmsConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "dms"

    def ready(self) -> None:
        from . import signals  # noqa: F401
