import json
from django.contrib import admin, messages
from django.utils.safestring import mark_safe
from django.utils.html import format_html, format_html_join
from django.utils import timezone
from django.db.models import Count, Q
from django.db import router
from datetime import timedelta
from .models import TaskRun, Worker, PeriodicTask, RateLimit, WorkflowRun, QueueControl
from .recurring import periodic_definition_status

def format_json(field_data):
    return format_html('<pre>{}</pre>', json.dumps(field_data, indent=2, sort_keys=True))

class LeaseStatusFilter(admin.SimpleListFilter):
    title = "Lease"
    parameter_name = "lease_status"

    def lookups(self, request, model_admin):
        return (
            ("stale", "Stale (expired)"),
            ("active", "Active"),
            ("none", "None"),
        )

    def queryset(self, request, queryset):
        now = timezone.now()
        if self.value() == "stale":
            return queryset.filter(status="RUNNING", leased_until__lt=now)
        if self.value() == "active":
            return queryset.filter(status="RUNNING", leased_until__gte=now)
        if self.value() == "none":
            return queryset.filter(status="RUNNING", leased_until__isnull=True)
        return queryset

@admin.register(Worker)
class WorkerAdmin(admin.ModelAdmin):
    list_display = ("worker_id", "hostname_display", "concurrency", "queues", "status_icon", "last_seen_at")
    readonly_fields = ("worker_id", "hostname", "concurrency", "queues", "started_at", "last_seen_at", "version")
    actions = ["delete_stale_workers"]
    
    def hostname_display(self, obj):
        return f"{obj.hostname} (v{obj.version or '?'})"
    hostname_display.short_description = "Worker Info"

    def status_icon(self, obj):
        is_alive = obj.last_seen_at > timezone.now() - timedelta(minutes=2)
        color = "#2e7d32" if is_alive else "#d32f2f"
        label = "ALIVE" if is_alive else "DEAD"
        return format_html('<b style="color: {};">● {}</b>', color, label)
    status_icon.short_description = "Status"

    @admin.action(description="Delete stale workers (last seen > 10m)")
    def delete_stale_workers(self, request, queryset):
        cutoff = timezone.now() - timedelta(minutes=10)
        stale = queryset.filter(last_seen_at__lt=cutoff)
        count = stale.count()
        stale.delete()
        self.message_user(request, f"Deleted {count} stale worker(s).", messages.SUCCESS)

    def has_add_permission(self, request): return False


@admin.register(QueueControl)
class QueueControlAdmin(admin.ModelAdmin):
    list_display = ("queue_name", "paused", "paused_at", "reason", "updated_at")
    search_fields = ("queue_name", "reason")
    actions = ["pause_queues", "resume_queues"]

    @admin.action(description="Pause selected queues")
    def pause_queues(self, request, queryset):
        now = timezone.now()
        updated = queryset.update(paused=True, paused_at=now, updated_at=now)
        self.message_user(request, f"Paused {updated} queue(s).", messages.SUCCESS)

    @admin.action(description="Resume selected queues")
    def resume_queues(self, request, queryset):
        now = timezone.now()
        updated = queryset.update(paused=False, paused_at=None, updated_at=now)
        self.message_user(request, f"Resumed {updated} queue(s).", messages.SUCCESS)

@admin.register(PeriodicTask)
class PeriodicTaskAdmin(admin.ModelAdmin):
    list_display = ("name", "cron_expr", "task_path", "queue_name", "priority", "concurrency_display", "scheduler_provider", "registration_status", "next_occurrence", "enabled", "last_run_at")
    list_filter = ("enabled", "queue_name", "scheduler_provider")
    search_fields = ("name", "task_path")
    ordering = ("next_run_at",)

    def get_readonly_fields(self, request, obj=None):
        # Source intent/spec identity goes through register_periodic. Operators
        # may pause a definition here without accidentally reserializing it.
        return tuple(field.name for field in self.model._meta.fields if field.name != "enabled")

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        from .schema_contract import require_schema_contract
        from django.db import connections

        alias = obj._state.db or router.db_for_write(self.model, instance=obj)
        require_schema_contract(connections[alias])
        # Do not overwrite concurrent source reconciliation with stale readonly
        # field values loaded when this admin form was opened.
        changed = self.model.objects.using(alias).filter(pk=obj.pk).update(
            enabled=obj.enabled, updated_at=timezone.now()
        )
        if changed != 1:
            raise ValueError("Periodic definition no longer exists; reload before changing it.")

    @admin.display(description="Registration")
    def registration_status(self, obj):
        status = periodic_definition_status(obj.spec_version, obj.scheduler_provider)
        return "Reconciliation required" if status == "reconciliation_required" else "Registered"

    @admin.display(description="Next occurrence")
    def next_occurrence(self, obj):
        if obj.scheduler_provider == "pg_cron":
            return "Managed by native pg_cron"
        return obj.next_run_at

    def concurrency_display(self, obj):
        if obj.concurrency_limit and obj.concurrency_key:
            return f"{obj.concurrency_key} ({obj.concurrency_limit})"
        if obj.concurrency_limit:
            return f"limit={obj.concurrency_limit}"
        return "-"
    concurrency_display.short_description = "Concurrency"

@admin.register(WorkflowRun)
class WorkflowRunAdmin(admin.ModelAdmin):
    list_display = ("workflow_id", "status", "expected_count", "success_count", "failure_count", "callback_result_id", "updated_at")
    search_fields = ("workflow_id", "callback_result_id")
    ordering = ("-updated_at",)
    readonly_fields = tuple(field.name for field in WorkflowRun._meta.fields)

    def has_add_permission(self, request): return False

    def has_delete_permission(self, request, obj=None): return False

@admin.register(RateLimit)
class RateLimitAdmin(admin.ModelAdmin):
    list_display = ("key", "tokens_per_second", "burst_size", "current_tokens", "last_refilled_at")
    search_fields = ("key",)
    ordering = ("key",)

@admin.register(TaskRun)
class TaskRunAdmin(admin.ModelAdmin):
    list_display = (
        "result_id", "status_badge", "lease_status", "lock_key", "concurrency_info", "queue_name", "priority",
        "enqueued_at", "duration", "attempts_display", "workflow_info"
    )
    list_filter = ("status", LeaseStatusFilter, "queue_name", "backend_alias")
    search_fields = ("result_id", "spec_hash", "task_path", "leased_by", "lock_key", "workflow_id")

    def get_queryset(self, request):
        return super().get_queryset(request).defer(
            "spec_json",
            "errors_json",
            "return_json",
            "worker_ids",
        )
    
    def workflow_info(self, obj):
        if not obj.workflow_id:
            return "-"
        return format_html(
            '<small>ID: {}</small><br><small>Wait: {} | Parent: {}</small>',
            str(obj.workflow_id)[:8], obj.wait_count, obj.parent_id or "None"
        )
    workflow_info.short_description = "Workflow"
    readonly_fields = [f.name for f in TaskRun._meta.fields] + [
        "pretty_spec", "pretty_errors", "pretty_return", "duplicate_specs"
    ]
    
    actions = [
        "replay_tasks",
        "retry_failed_tasks",
        "cancel_tasks",
    ]
    def status_badge(self, obj):
        colors = {
            "READY": "#777",
            "RUNNING": "#2271b1",
            "SUCCESSFUL": "#2e7d32",
            "FAILED": "#d32f2f",
            "CANCELLED": "#ff9800",
        }
        color = colors.get(obj.status, "#000")
        return format_html(
            '<span style="background: {}; color: white; padding: 2px 8px; border-radius: 12px; font-size: 10px; font-weight: bold;">{}</span>',
            color, obj.status
        )
    status_badge.short_description = "Status"

    def lease_status(self, obj):
        if obj.status != "RUNNING":
            return "-"
        if not obj.leased_until:
            return format_html('<b style="color: #9e9e9e;">NONE</b>')
        if obj.leased_until < timezone.now():
            return format_html('<b style="color: #d32f2f;">STALE</b>')
        return format_html('<b style="color: #2e7d32;">ACTIVE</b>')
    lease_status.short_description = "Lease"

    def duration(self, obj):
        if obj.started_at and obj.finished_at:
            diff = obj.finished_at - obj.started_at
            return f"{diff.total_seconds():.2f}s"
        if obj.started_at:
            diff = timezone.now() - obj.started_at
            return f"{diff.total_seconds():.1f}s..."
        return "-"

    def concurrency_info(self, obj):
        if obj.concurrency_limit and obj.concurrency_key:
            return f"{obj.concurrency_key} ({obj.concurrency_limit})"
        if obj.concurrency_limit:
            return f"limit={obj.concurrency_limit}"
        return "-"
    concurrency_info.short_description = "Concurrency"
    
    def attempts_display(self, obj):
        return f"{obj.attempts}/{obj.max_attempts}"
    attempts_display.short_description = "Atts"

    def pretty_spec(self, obj): return format_json(obj.spec_json)
    def pretty_errors(self, obj): return format_json(obj.errors_json)
    def pretty_return(self, obj): return format_json(obj.return_json)

    def duplicate_specs(self, obj):
        others = TaskRun.objects.using(obj._state.db).filter(spec_hash=obj.spec_hash).exclude(result_id=obj.result_id)
        if not others.exists():
            return "No other runs share this spec_hash."
        return format_html_join('<br>', '<a href="{}">{}</a> ({})',
                                ((f'../{other.result_id}/change/', other.result_id, other.status)
                                 for other in others))
    duplicate_specs.short_description = "Other runs with same spec_hash"

    def changelist_view(self, request, extra_context=None):
        stats = self.get_queryset(request).aggregate(
            running=Count("result_id", filter=Q(status="RUNNING")),
            failed=Count("result_id", filter=Q(status="FAILED")),
            ready=Count("result_id", filter=Q(status="READY")),
        )
        
        refresh_script = """
            <script>
                if (!window.location.search.includes('popup=1')) {
                    setTimeout(function(){ window.location.reload(); }, 10000);
                }
            </script>
        """
        
        summary_html = format_html(
            '<div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 20px; display: flex; gap: 30px; border: 1px solid #ddd; font-family: sans-serif;">'
            '<div style="text-align: center;"><small style="color: #666; text-transform: uppercase; font-weight: bold; font-size: 10px;">Ready</small><br><b style="font-size: 20px;">{}</b></div>'
            '<div style="text-align: center;"><small style="color: #666; text-transform: uppercase; font-weight: bold; font-size: 10px;">Running</small><br><b style="font-size: 20px; color: #2271b1;">{}</b></div>'
            '<div style="text-align: center;"><small style="color: #666; text-transform: uppercase; font-weight: bold; font-size: 10px;">Failed (Total)</small><br><b style="font-size: 20px; color: #d32f2f;">{}</b></div>'
            '<div style="margin-left: auto; align-self: center;"><small style="color: #999;">Auto-refreshing every 10s</small></div>'
            '{}'
            '</div>',
            stats['ready'], stats['running'], stats['failed'],
            mark_safe(refresh_script)
        )
        
        extra_context = extra_context or {}
        extra_context['summary_stats'] = summary_html
        return super().changelist_view(request, extra_context=extra_context)

    def _request_operation(self, request, queryset, operation):
        from .operations import OperationError, preflight_task_operations, task_operation

        submitted = 0
        try:
            identifiers = preflight_task_operations(queryset, operation)
            for identifier in identifiers:
                task_operation(operation, identifier, using=queryset.db)
                submitted += 1
        except (OperationError, ValueError) as error:
            self.message_user(request, str(error), messages.ERROR)
        self.message_user(request, f'Submitted {submitted} {operation} request(s); inspect current task state.', messages.INFO)

    def has_replay_permission(self, request):
        return request.user.has_perm('reproq_django.can_replay_taskrun')

    @admin.action(description="Replay selected tasks", permissions=['replay'])
    def replay_tasks(self, request, queryset):
        self._request_operation(request, queryset, 'replay')

    @admin.action(description="Retry selected failed tasks")
    def retry_failed_tasks(self, request, queryset):
        self._request_operation(request, queryset.filter(status='FAILED'), 'retry')

    @admin.action(description="Cancel selected tasks")
    def cancel_tasks(self, request, queryset):
        self._request_operation(request, queryset, 'cancel')

    def has_delete_permission(self, request, obj=None): return False

    def has_add_permission(self, request): return False
