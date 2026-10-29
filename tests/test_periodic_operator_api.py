import json
from types import SimpleNamespace

from django.core.management import call_command
from django.test import RequestFactory
from django.utils import timezone

from reproq_django.models import PeriodicTask
from reproq_django.views import reproq_stats_api


def test_stats_distinguish_legacy_registration_and_native_schedule_clock():
    call_command('migrate', verbosity=0)
    PeriodicTask.objects.all().delete()
    try:
        for name, provider, version in [('legacy', 'unassigned', 0), ('native', 'pg_cron', 1), ('go', 'go', 1)]:
            PeriodicTask.objects.create(name=name, cron_expr='0 0 * * *', task_path='owned.task',
                                        next_run_at=timezone.now(), scheduler_provider=provider, spec_version=version)
        request = RequestFactory().get('/stats/')
        request.user = SimpleNamespace(is_authenticated=True, is_staff=True)
        response = reproq_stats_api(request)
        rows = {row['name']: row for row in json.loads(response.content)['periodic']}
        assert rows['legacy']['definition_status'] == 'reconciliation_required'
        assert rows['native']['definition_status'] == 'registered'
        assert rows['native']['next_run_at'] is None
        assert 'execution minute' in rows['native']['occurrence_clock']
        assert rows['go']['next_run_at'] is not None
    finally:
        PeriodicTask.objects.all().delete()
