# Architecture

Django defines tasks and canonical serialized specs. Committed Django migrations
own the six queue tables, constraints, contract marker and shared periodic enqueue
function. Explicit audited adoption supports known legacy catalogs/history;
neither workers, login hooks nor management helpers repair queue schemas.

Go owns claim, heartbeat, task process lifecycle, retry/reclaim and workflow
finalization. Startup verifies schema2/executor protocol2 before registration or
claims. Claims use SKIP LOCKED plus per-key concurrency arbitration and a fresh
UUID lease token. RUNNING owner/token/unexpired database-clock lease fence state
changes. Python's TaskContext uses the same fence for progress metadata.

Go starts a bounded-lifetime Python provider per active task. Setup/import/hooks
and native task writes are drained as logs on stderr; stdout contains exactly
one complete bounded JSON result. Timeout/cancellation includes the owned process
tree. Metadata/result ownership is rechecked before success publication. Expired
leases and manual reclaim share Go transitions; SQLite is a local SDK test target,
not a Go production queue backend.

Delivery is at least once. A token prevents stale queue/metadata writes; it does
not undo an external side effect already performed by a task. Applications need
idempotency keys or a transaction for those effects and for data publication.

One SDK spec builder serves direct/bulk enqueue and explicit periodic
registration. Go and optional pg_cron call the same migration-owned occurrence
primitive, copying stored canonical text/hash instead of serializing another
spec. Every periodic definition has a provider. Go coalesces one overdue nominal
tick then advances from the DB clock. Native pg_cron records the UTC execution
minute and follows its native catch-up behavior; its global cron.timezone must
match the definition. Occurrence uniqueness survives terminal task states.

A small deployment can use one worker slot and one-shot scheduling, or pg_cron
where the database actually supports it. Larger deployments add workers/queue
routing without another task API. Measure Python task memory, DB connections,
queue delay, task duration and end-to-end response latency before increasing
concurrency. A compact Go coordinator cannot remove the memory cost of ETL.

```text
Django direct/bulk specs ----> task_runs <---- shared periodic enqueue function
                                   ^                       ^
                                   |                 Go beat / pg_cron
                         Go worker claim/token
                                   |
                         Python executor process
                                   |
                       fenced result / retry / failure
```
