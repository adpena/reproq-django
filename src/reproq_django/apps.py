from django.apps import AppConfig
from django.db.models.signals import post_migrate

from .memlog import start_memory_logger
from .recurring import maybe_sync_recurring_tasks


class ReproqDjangoConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "reproq_django"
    verbose_name = "Reproq Django Tasks"

    def ready(self) -> None:
        start_memory_logger()
        post_migrate.connect(
            maybe_sync_recurring_tasks,
            sender=self,
            dispatch_uid="reproq_recurring_sync",
        )
