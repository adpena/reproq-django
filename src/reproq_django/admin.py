import hashlib
import json
import uuid
from django.contrib import admin, messages
from django.utils.safestring import mark_safe
from django.utils.html import format_html
from django.utils import timezone
from django.db.models import Count, Q
from datetime import timedelta
from django.urls import path, reverse
from django.http import HttpResponseRedirect
from .models import TaskRun, Worker, PeriodicTask

def format_json(field_data):
    if not field_data:
        return mark_safe("<pre>{}</pre>")
    res = json.dumps(field_data, indent=2, sort_keys=True)
    return mark_safe(f"<pre>{res}</pre>")

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
    
    def hostname_display(self, obj):
        return f"{obj.hostname} (v{obj.version or '?'})"
    hostname_display.short_description = "Worker Info"

    def status_icon(self, obj):
        is_alive = obj.last_seen_at > timezone.now() - timedelta(minutes=2)
        color = "#2e7d32" if is_alive else "#d32f2f"
        label = "ALIVE" if is_alive else "DEAD"
        return format_html('<b style="color: {};">● {}</b>', color, label)
    status_icon.short_description = "Status"

    def has_add_permission(self, request): return False

@admin.register(PeriodicTask)
class PeriodicTaskAdmin(admin.ModelAdmin):
    list_display = ("name", "cron_expr", "task_path", "next_run_at", "enabled", "last_run_at")
    list_filter = ("enabled", "queue_name")
    search_fields = ("name", "task_path")
    ordering = ("next_run_at",)

@admin.register(TaskRun)
class TaskRunAdmin(admin.ModelAdmin):
    list_display = (
        "result_id", "status_badge", "lease_status", "lock_key", "queue_name", "priority", 
        "enqueued_at", "duration", "attempts_display", "workflow_info"
    )
    list_filter = ("status", LeaseStatusFilter, "queue_name", "backend_alias")
    search_fields = ("result_id", "spec_hash", "leased_by", "lock_key", "workflow_id")
    
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
        "create_expired_lease_test_task",
    ]
    change_list_template = "admin/reproq_django/taskrun/change_list.html"

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "create-expired-lease/",
                self.admin_site.admin_view(self.create_expired_lease_view),
                name="reproq_django_taskrun_create_expired_lease",
            )
        ]
        return custom_urls + urls

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
    
    def attempts_display(self, obj):
        return f"{obj.attempts}/{obj.max_attempts}"
    attempts_display.short_description = "Atts"

    def pretty_spec(self, obj): return format_json(obj.spec_json)
    def pretty_errors(self, obj): return format_json(obj.errors_json)
    def pretty_return(self, obj): return format_json(obj.return_json)

    def duplicate_specs(self, obj):
        others = TaskRun.objects.filter(spec_hash=obj.spec_hash).exclude(result_id=obj.result_id)
        if not others.exists():
            return "No other runs share this spec_hash."
        links = []
        for other in others:
            url = f"../{other.result_id}/change/"
            links.append(f'<a href="{url}">{other.result_id}</a> ({other.status})')
        return mark_safe("<br>".join(links))
    duplicate_specs.short_description = "Other runs with same spec_hash"

    def changelist_view(self, request, extra_context=None):
        stats = TaskRun.objects.aggregate(
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

    @admin.action(description="Replay selected tasks (creates new READY runs)")
    def replay_tasks(self, request, queryset):
        count = 0
        for old_run in queryset:
            TaskRun.objects.create(
                backend_alias=old_run.backend_alias,
                queue_name=old_run.queue_name,
                priority=old_run.priority,
                run_after=None,
                spec_json=old_run.spec_json,
                spec_hash=old_run.spec_hash,
                status="READY",
                errors_json=[],
                attempts=0,
                max_attempts=old_run.max_attempts,
                timeout_seconds=old_run.timeout_seconds
            )
            count += 1
        self.message_user(request, f"Successfully replayed {count} tasks.", messages.SUCCESS)

    @admin.action(description="Retry failed tasks (sets status to READY)")
    def retry_failed_tasks(self, request, queryset):
        updated = queryset.filter(status="FAILED").update(status="READY", run_after=timezone.now())
        self.message_user(request, f"Successfully set {updated} failed tasks to READY.", messages.SUCCESS)

    @admin.action(description="Cancel selected tasks")
    def cancel_tasks(self, request, queryset):
        updated = queryset.filter(status__in=["READY", "RUNNING"]).update(cancel_requested=True)
        queryset.filter(status="READY").update(status="CANCELLED")
        self.message_user(request, f"Requested cancellation for {updated} tasks.", messages.SUCCESS)

    @admin.action(description="Create expired-lease test task (for reclaim)")
    def create_expired_lease_test_task(self, request, queryset):
        run = self._create_expired_lease_task()
        self.message_user(
            request,
            f"Created expired-lease test task (result_id={run.result_id}).",
            messages.SUCCESS,
        )

    def create_expired_lease_view(self, request):
        run = self._create_expired_lease_task()
        self.message_user(
            request,
            f"Created expired-lease test task (result_id={run.result_id}).",
            messages.SUCCESS,
        )
        return HttpResponseRedirect(reverse("admin:reproq_django_taskrun_changelist"))

    def _create_expired_lease_task(self):
        spec = {
            "v": 1,
            "task_path": "reproq_django.tasks.debug_noop_task",
            "args": [],
            "kwargs": {"sleep_seconds": 0},
            "takes_context": False,
            "queue_name": "default",
            "priority": 0,
            "run_after": None,
            "exec": {
                "timeout_seconds": 900,
                "max_attempts": 3,
            },
            "provenance": {
                "reclaim_test_id": str(uuid.uuid4()),
            },
        }
        spec_str = json.dumps(spec, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        spec_hash = hashlib.sha256(spec_str.encode("utf-8")).hexdigest()
        return TaskRun.objects.create(
            backend_alias="default",
            queue_name=spec["queue_name"],
            priority=spec["priority"],
            run_after=None,
            spec_json=spec,
            spec_hash=spec_hash,
            status="RUNNING",
            errors_json=[],
            attempts=0,
            max_attempts=spec["exec"]["max_attempts"],
            timeout_seconds=spec["exec"]["timeout_seconds"],
            leased_until=timezone.now() - timedelta(minutes=10),
            leased_by="admin-expired-lease",
        )

    def has_add_permission(self, request): return False
