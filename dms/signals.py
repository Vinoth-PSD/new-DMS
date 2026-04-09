from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import ResourceProfile
from .tasks import assign_pages_task


@receiver(post_save, sender=ResourceProfile)
def auto_assign_when_resource_active(sender, instance: ResourceProfile, **kwargs):
    if instance.is_active_session and instance.remaining_capacity > 0:
        assign_pages_task.delay()
