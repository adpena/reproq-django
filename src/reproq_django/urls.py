from django.urls import path
from .tui_auth import tui_authorize, tui_config, tui_login, tui_pair, tui_pair_status
from .views import (
    reproq_stats_api,
    reproq_stress_test_api,
    reproq_tui_events_proxy,
    reproq_tui_health_proxy,
    reproq_tui_metrics_proxy,
)

urlpatterns = [
    path("stats/", reproq_stats_api, name="stats"),
    path("stress-test/", reproq_stress_test_api, name="stress-test"),
    path("tui/pair/", tui_pair, name="reproq-tui-pair"),
    path("tui/pair/<str:code>/", tui_pair_status, name="reproq-tui-pair-status"),
    path("tui/config/", tui_config, name="reproq-tui-config"),
    path("tui/metrics/", reproq_tui_metrics_proxy, name="reproq-tui-metrics"),
    path("tui/healthz/", reproq_tui_health_proxy, name="reproq-tui-health"),
    path("tui/events/", reproq_tui_events_proxy, name="reproq-tui-events"),
    path("tui/authorize/", tui_authorize, name="reproq-tui-authorize"),
    path("tui/login/", tui_login, name="reproq-tui-login"),
]
