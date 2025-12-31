import logging
import os
import sys
import threading

try:
    import resource
except ImportError:  # pragma: no cover - unavailable on some platforms
    resource = None

logger = logging.getLogger(__name__)
_started = False


def _parse_interval_seconds(value):
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    lowered = raw.lower()
    if lowered in ("0", "off", "false", "no"):
        return None
    unit = lowered[-1]
    multiplier = 1
    if unit in ("s", "m", "h"):
        raw = raw[:-1]
        if not raw:
            return None
        if unit == "m":
            multiplier = 60
        elif unit == "h":
            multiplier = 3600
    try:
        seconds = float(raw) * multiplier
    except ValueError:
        return None
    if seconds <= 0:
        return None
    return seconds


def _read_proc_rss_bytes():
    if not sys.platform.startswith("linux"):
        return None
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1]) * 1024
    except OSError:
        return None
    return None


def _read_ru_maxrss_bytes():
    if resource is None:
        return None
    try:
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return None
    if value <= 0:
        return None
    if sys.platform == "darwin":
        return int(value)
    return int(value) * 1024


def _log_memory_usage(interval_seconds):
    pid = os.getpid()
    rss_bytes = _read_proc_rss_bytes()
    ru_maxrss_bytes = _read_ru_maxrss_bytes()
    logger.info(
        "reproq_memory_usage pid=%s rss_bytes=%s ru_maxrss_bytes=%s threads=%s interval_s=%s",
        pid,
        rss_bytes,
        ru_maxrss_bytes,
        threading.active_count(),
        interval_seconds,
    )


def _memory_loop(interval_seconds, stop_event):
    _log_memory_usage(interval_seconds)
    while not stop_event.wait(interval_seconds):
        try:
            _log_memory_usage(interval_seconds)
        except Exception:
            logger.exception("reproq_memory_usage failed")


def start_memory_logger():
    global _started
    if _started:
        return
    interval = _parse_interval_seconds(os.environ.get("REPROQ_MEMORY_LOG_INTERVAL", ""))
    if not interval:
        return
    _started = True
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_memory_loop,
        args=(interval, stop_event),
        daemon=True,
        name="reproq-memory-logger",
    )
    thread.start()
