from django.apps import AppConfig

from .memlog import start_memory_logger


class ReproqDjangoConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "reproq_django"
    verbose_name = "Reproq Django Tasks"

    def ready(self) -> None:
        start_memory_logger()
