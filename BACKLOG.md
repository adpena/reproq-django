# Reproq Django Backlog

## Code Alignment & Reliability
- [x] Remove or shim the duplicate `reproq_django/models.py` at repo root to avoid import shadowing; keep all models in `src/reproq_django/models.py`.
- [x] Decide whether `lock_key` should participate in `spec_hash`; if yes, include it in the canonical spec before hashing, or document the current dedup behavior as an intentional constraint.
- [x] Make `bulk_enqueue` respect `DEDUP_ACTIVE` (or warn/guard against the unique index conflict when enabled).
- [x] Use a stable JSON encoder (e.g., DjangoJSONEncoder) for `spec_hash` computation to avoid failures on `Decimal`/`UUID` inputs, while preserving canonical ordering.

## Docs/Behavior Reconciliation
- [x] Ensure executor docs match supported inputs (payload modes, settings module resolution, and stdout/stderr handling).
- [x] Ensure settings docs match actual behavior in `manage.py reproq` (binary resolution order, DSN derivation, and queue flags).

## Partial/Unimplemented Features (from current docs)
- [x] Support `enqueue(priority=...)` overrides (currently documented as not supported).
- [x] Wire `python manage.py reproq worker --queue` to the Go worker (mapped to `--queues`).

## Progress log
- 2025-12-30: Completed backlog items (lock_key in spec hash, stable encoder, bulk enqueue dedupe, priority override, docs alignment, remove root models shim).
