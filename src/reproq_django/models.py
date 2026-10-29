from django.db import models
from django.db.models.functions import Now


class TaskRunStatus:
    READY = "READY"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class TaskRun(models.Model):
    result_id = models.BigAutoField(primary_key=True)
    backend_alias = models.TextField(default="default", db_default="default")
    queue_name = models.TextField(default="default", db_default="default")
    priority = models.IntegerField(default=0, db_default=0)
    run_after = models.DateTimeField(null=True, blank=True)

    spec_json = models.JSONField()
    spec_hash = models.CharField(max_length=64)
    canonical_spec_text = models.TextField(null=True, blank=True)
    spec_version = models.PositiveIntegerField(default=0, db_default=0)
    dedupe_active = models.BooleanField(default=True, db_default=True)
    periodic_name = models.TextField(null=True, blank=True)
    scheduled_for = models.DateTimeField(null=True, blank=True)
    periodic_provider = models.TextField(null=True, blank=True)
    task_path = models.TextField(null=True, blank=True)

    status = models.TextField(default=TaskRunStatus.READY, db_default=TaskRunStatus.READY)
    enqueued_at = models.DateTimeField(auto_now_add=True, db_default=Now())
    started_at = models.DateTimeField(null=True, blank=True)
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    attempts = models.IntegerField(default=0, db_default=0)
    max_attempts = models.IntegerField(default=3, db_default=3)
    timeout_seconds = models.IntegerField(default=900, db_default=900)
    
    lock_key = models.TextField(null=True, blank=True)
    concurrency_key = models.TextField(null=True, blank=True)
    concurrency_limit = models.IntegerField(default=0, db_default=0)
    parent = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True, related_name='children')
    workflow_id = models.UUIDField(null=True, blank=True)
    wait_count = models.IntegerField(default=0, db_default=0)
    
    # Use JSONField instead of ArrayField for SQLite compatibility
    worker_ids = models.JSONField(default=list, blank=True, db_default=[])

    return_json = models.JSONField(null=True, blank=True)
    errors_json = models.JSONField(default=list, blank=True, db_default=[])
    metadata_json = models.JSONField(default=dict, blank=True, db_default={})
    last_error = models.TextField(null=True, blank=True)
    failed_at = models.DateTimeField(null=True, blank=True)

    leased_until = models.DateTimeField(null=True, blank=True)
    leased_by = models.TextField(null=True, blank=True)
    lease_token = models.UUIDField(null=True, blank=True)

    logs_uri = models.TextField(null=True, blank=True)
    artifacts_uri = models.TextField(null=True, blank=True)

    expires_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_default=Now())
    updated_at = models.DateTimeField(auto_now=True, db_default=Now())
    
    cancel_requested = models.BooleanField(default=False, db_default=False)

    class Meta:
        db_table = "task_runs"
        verbose_name = "Task Run"
        verbose_name_plural = "Task Runs"
        permissions = [
            ("can_replay_taskrun", "Can replay task run"),
        ]
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["queue_name", "status"]),
            models.Index(fields=["concurrency_key", "status"], name="task_runs_conc_status_idx"),
        ]
        constraints = [
            models.CheckConstraint(
                condition=models.Q(periodic_provider__isnull=True) | models.Q(periodic_provider__in=["go", "pg_cron"]),
                name="reproq_periodic_provider_valid",
            ),
            models.UniqueConstraint(
                fields=["spec_hash"],
                condition=models.Q(
                    status__in=["READY", "RUNNING"], periodic_name__isnull=True,
                    dedupe_active=True,
                ),
                name="reproq_active_spec_unique",
            ),
            models.UniqueConstraint(
                fields=["periodic_name", "scheduled_for"],
                condition=models.Q(periodic_name__isnull=False),
                name="reproq_periodic_occurrence_unique",
            ),
            models.CheckConstraint(
                condition=(models.Q(periodic_name__isnull=True, scheduled_for__isnull=True)
                           | models.Q(periodic_name__isnull=False, scheduled_for__isnull=False)),
                name="reproq_periodic_occurrence_pair",
            ),
            models.CheckConstraint(
                condition=(
                    (models.Q(status="RUNNING", lease_token__isnull=False,
                              leased_by__isnull=False, leased_until__isnull=False)
                     & ~models.Q(leased_by=""))
                    | (~models.Q(status="RUNNING") & models.Q(lease_token__isnull=True))
                ),
                name="reproq_running_lease_fenced",
            ),
        ]

    def __str__(self):
        return f"{self.result_id} ({self.status})"

class Worker(models.Model):
    worker_id = models.TextField(primary_key=True)
    hostname = models.TextField(null=True, blank=True)
    concurrency = models.IntegerField(null=True, blank=True)
    queues = models.JSONField(default=list, db_default=[])
    started_at = models.DateTimeField(auto_now_add=True, db_default=Now())
    last_seen_at = models.DateTimeField(auto_now=True, db_default=Now())
    version = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "reproq_workers"
        verbose_name = "Worker"
        verbose_name_plural = "Workers"

    def __str__(self):
        return f"{self.worker_id} ({self.hostname})"

class PeriodicTask(models.Model):
    name = models.TextField(primary_key=True)
    cron_expr = models.TextField()
    task_path = models.TextField()
    payload_json = models.JSONField(default=dict, blank=True, db_default={})
    canonical_spec_text = models.TextField(null=True, blank=True)
    canonical_spec_hash = models.CharField(max_length=64, null=True, blank=True)
    spec_version = models.PositiveIntegerField(default=0, db_default=0)
    scheduler_provider = models.TextField(default="unassigned", db_default="unassigned",
                                        choices=[("unassigned", "Unassigned"), ("go", "Go"), ("pg_cron", "pg_cron")])
    queue_name = models.TextField(default="default", db_default="default")
    priority = models.IntegerField(default=0, db_default=0)
    max_attempts = models.IntegerField(default=3, db_default=3)
    concurrency_key = models.TextField(null=True, blank=True)
    concurrency_limit = models.IntegerField(default=0, db_default=0)
    last_run_at = models.DateTimeField(null=True, blank=True)
    next_run_at = models.DateTimeField()
    enabled = models.BooleanField(default=True, db_default=True)
    created_at = models.DateTimeField(auto_now_add=True, db_default=Now())
    updated_at = models.DateTimeField(auto_now=True, db_default=Now())

    class Meta:
        db_table = "periodic_tasks"
        verbose_name = "Periodic Task"
        verbose_name_plural = "Periodic Tasks"
        constraints = [models.CheckConstraint(
            condition=models.Q(scheduler_provider__in=["unassigned", "go", "pg_cron"]),
            name="reproq_scheduler_provider_valid",
        )]

    def __str__(self):
        return f"{self.name} ({self.cron_expr})"

class QueueControl(models.Model):
    queue_name = models.TextField(primary_key=True)
    paused = models.BooleanField(default=False, db_default=False)
    paused_at = models.DateTimeField(null=True, blank=True)
    reason = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_default=Now())
    updated_at = models.DateTimeField(auto_now=True, db_default=Now())

    class Meta:
        db_table = "reproq_queue_controls"
        verbose_name = "Queue Control"
        verbose_name_plural = "Queue Controls"

    def __str__(self):
        return self.queue_name

class WorkflowRun(models.Model):
    workflow_id = models.UUIDField(primary_key=True)
    expected_count = models.IntegerField()
    success_count = models.IntegerField(default=0, db_default=0)
    failure_count = models.IntegerField(default=0, db_default=0)
    callback_result_id = models.BigIntegerField(null=True, blank=True)
    status = models.TextField(default="RUNNING", db_default="RUNNING")
    created_at = models.DateTimeField(db_default=Now())
    updated_at = models.DateTimeField(db_default=Now())

    class Meta:
        db_table = "workflow_runs"
        verbose_name = "Workflow Run"
        verbose_name_plural = "Workflow Runs"

    def __str__(self):
        return str(self.workflow_id)

class RateLimit(models.Model):
    key = models.TextField(primary_key=True)
    tokens_per_second = models.FloatField()
    burst_size = models.IntegerField()
    current_tokens = models.FloatField()
    last_refilled_at = models.DateTimeField(db_default=Now())

    class Meta:
        db_table = "rate_limits"
        verbose_name = "Rate Limit"
        verbose_name_plural = "Rate Limits"

    def __str__(self):
        return self.key


class SchemaContract(models.Model):
    """Compatibility marker written exclusively by the committed migration."""

    singleton_id = models.PositiveSmallIntegerField(primary_key=True)
    version = models.PositiveIntegerField()
    contract_id = models.TextField()
    migration_identity = models.TextField()
    capabilities = models.JSONField()
    applied_at = models.DateTimeField(db_default=Now())

    class Meta:
        db_table = "reproq_schema_contract"
        constraints = [models.CheckConstraint(
            condition=models.Q(singleton_id=1), name="reproq_schema_contract_singleton"
        )]
