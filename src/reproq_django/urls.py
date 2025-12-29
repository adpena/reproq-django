from django.urls import path
from .views import reproq_stats_api, reproq_stress_test_api

urlpatterns = [
    path("stats/", reproq_stats_api, name="stats"),
    path("stress-test/", reproq_stress_test_api, name="stress-test"),
]
