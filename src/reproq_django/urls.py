from django.urls import path
from .tui_auth import tui_authorize, tui_login, tui_pair, tui_pair_status
from .views import reproq_stats_api, reproq_stress_test_api

urlpatterns = [
    path("stats/", reproq_stats_api, name="stats"),
    path("stress-test/", reproq_stress_test_api, name="stress-test"),
    path("tui/pair/", tui_pair, name="reproq-tui-pair"),
    path("tui/pair/<str:code>/", tui_pair_status, name="reproq-tui-pair-status"),
    path("tui/authorize/", tui_authorize, name="reproq-tui-authorize"),
    path("tui/login/", tui_login, name="reproq-tui-login"),
]
