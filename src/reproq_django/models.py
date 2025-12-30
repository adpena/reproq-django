from django.db import models

class TaskRun(models.Model):
    result_id = models.BigAutoField(primary_key=True)
    backend_alias = models.TextField(default="default")
    queue_name = models.TextField(default="default")
    priority = models.IntegerField(default=0)
    run_after = models.DateTimeField(null=True, blank=True)

    spec_json = models.JSONField()
    spec_hash = models.CharField(max_length=64)

    status = models.TextField(default="READY")
    enqueued_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    attempts = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=3)
    timeout_seconds = models.IntegerField(default=900)
    
    lock_key = models.TextField(null=True, blank=True)
    parent = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True, related_name='children')
    workflow_id = models.UUIDField(null=True, blank=True)
    wait_count = models.IntegerField(default=0)
    
    # Use JSONField instead of ArrayField for SQLite compatibility
    worker_ids = models.JSONField(default=list, blank=True)

    return_json = models.JSONField(null=True, blank=True)
    errors_json = models.JSONField(default=list, blank=True)

    leased_until = models.DateTimeField(null=True, blank=True)
    leased_by = models.TextField(null=True, blank=True)

    logs_uri = models.TextField(null=True, blank=True)
    artifacts_uri = models.TextField(null=True, blank=True)

    expires_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    cancel_requested = models.BooleanField(default=False)

    class Meta:
        db_table = "task_runs"
        verbose_name = "Task Run"
        verbose_name_plural = "Task Runs"
        permissions = [
            ("can_replay_taskrun", "Can replay task run"),
        ]

    def __str__(self):
        return f"{self.result_id} ({self.status})"

class Worker(models.Model):
    worker_id = models.TextField(primary_key=True)
    hostname = models.TextField()
    concurrency = models.IntegerField()
    queues = models.JSONField(default=list)
    started_at = models.DateTimeField(auto_now_add=True)
    last_seen_at = models.DateTimeField(auto_now=True)
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
    payload_json = models.JSONField(default=dict, blank=True)
    queue_name = models.TextField(default="default")
    priority = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=3)
    last_run_at = models.DateTimeField(null=True, blank=True)
    next_run_at = models.DateTimeField()
    enabled = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "periodic_tasks"
        verbose_name = "Periodic Task"
        verbose_name_plural = "Periodic Tasks"

    def __str__(self):
        return f"{self.name} ({self.cron_expr})"

class RateLimit(models.Model):
    key = models.TextField(primary_key=True)
    tokens_per_second = models.FloatField()
    burst_size = models.IntegerField()
    current_tokens = models.FloatField()
    last_refilled_at = models.DateTimeField()

    class Meta:
        db_table = "rate_limits"
        verbose_name = "Rate Limit"
        verbose_name_plural = "Rate Limits"
        managed = False

    def __str__(self):
        return self.key
