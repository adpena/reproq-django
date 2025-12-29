import time
import logging
from django.tasks import task

logger = logging.getLogger(__name__)

@task()
def debug_noop_task(sleep_seconds: float = 0):
    """
    A simple task that does nothing, used for stress testing and benchmarks.
    """
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    return "ok"

@task()
def debug_error_task(message: str = "Simulated task failure"):
    """
    A task that always fails, used to test error handling and retries.
    """
    raise RuntimeError(message)
