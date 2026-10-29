CREATE FUNCTION public.reproq_enqueue_periodic_occurrence(
    p_name text, p_scheduled_for timestamp with time zone, p_provider text
)
RETURNS TABLE(result_id bigint, inserted boolean)
LANGUAGE plpgsql VOLATILE PARALLEL UNSAFE SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $reproq_occurrence_v1$
DECLARE
    v_definition public.periodic_tasks%ROWTYPE;
    v_document json;
    v_spec jsonb;
    v_invalid_document boolean;
    v_result_id bigint;
    v_timeout integer;
    v_relation record;
    v_marker_count integer;
BEGIN
    IF p_name IS NULL OR p_name = '' OR p_provider IS NULL OR p_provider NOT IN ('go', 'pg_cron')
       OR p_scheduled_for IS NULL OR NOT pg_catalog.isfinite(p_scheduled_for) THEN
        RAISE EXCEPTION 'A named periodic definition, finite occurrence time and assigned provider are required';
    END IF;
    -- All relation references below are explicitly public-qualified. A fixed,
    -- trusted search_path also prevents caller-supplied operators/functions
    -- from changing validation. This function never receives elevated rights.
    FOR v_relation IN
        SELECT requested.name, c.relkind, c.relpersistence, c.relispartition
        FROM pg_catalog.unnest(ARRAY['periodic_tasks', 'rate_limits', 'reproq_queue_controls',
            'reproq_workers', 'task_runs', 'workflow_runs', 'reproq_schema_contract', 'django_migrations']) requested(name)
        LEFT JOIN pg_catalog.pg_class c ON c.oid = pg_catalog.to_regclass('public.' || requested.name)
    LOOP
        IF v_relation.relkind IS DISTINCT FROM 'r'::"char"
           OR v_relation.relpersistence IS DISTINCT FROM 'p'::"char"
           OR v_relation.relispartition IS DISTINCT FROM false THEN
            RAISE EXCEPTION 'Periodic enqueue requires ordinary permanent public queue tables';
        END IF;
    END LOOP;
    SELECT count(*) INTO v_marker_count FROM public.reproq_schema_contract;
    IF v_marker_count <> 1 OR NOT EXISTS (
        SELECT 1 FROM public.reproq_schema_contract WHERE singleton_id = 1 AND version = 2
          AND contract_id = 'reproq-schema-v2'
          AND migration_identity = 'reproq_django.0003_periodic_enqueue_v1'
          AND capabilities = '["lease-token-v1","canonical-spec-v1","periodic-occurrence-v1","managed-six-tables-v1","periodic-enqueue-v1","scheduler-provider-v1"]'::jsonb
    ) OR (SELECT count(*) FROM public.django_migrations WHERE app = 'reproq_django'
          AND name = '0002_schema_contract_v1') <> 1
      OR (SELECT count(*) FROM public.django_migrations WHERE app = 'reproq_django'
          AND name = '0003_periodic_enqueue_v1') <> 1 THEN
        RAISE EXCEPTION 'Periodic enqueue requires the completed ReproQ schema v2 migration';
    END IF;
    SELECT * INTO v_definition FROM public.periodic_tasks WHERE name = p_name FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Periodic definition % does not exist', p_name;
    END IF;
    -- Returning an already-existing occurrence is read-only and remains
    -- idempotent after completion, disabling or a provider handover.
    SELECT tr.result_id INTO v_result_id FROM public.task_runs tr
        WHERE tr.periodic_name = p_name AND tr.scheduled_for = p_scheduled_for;
    IF FOUND THEN
        RETURN QUERY SELECT v_result_id, false;
        RETURN;
    END IF;
    IF NOT v_definition.enabled OR v_definition.scheduler_provider IS DISTINCT FROM p_provider THEN
        RAISE EXCEPTION 'Periodic definition is disabled or assigned to another scheduler provider';
    END IF;
    IF p_scheduled_for > pg_catalog.clock_timestamp() THEN
        RAISE EXCEPTION 'A future periodic occurrence cannot be enqueued';
    END IF;
    IF p_provider = 'go' AND p_scheduled_for IS DISTINCT FROM v_definition.next_run_at THEN
        RAISE EXCEPTION 'Go occurrence must match the definition nominal next_run_at';
    END IF;
    IF p_provider = 'pg_cron' AND p_scheduled_for IS DISTINCT FROM
        (pg_catalog.date_trunc('minute', p_scheduled_for AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') THEN
        RAISE EXCEPTION 'pg_cron occurrence identity requires a UTC trigger minute';
    END IF;
    IF v_definition.spec_version <> 1 OR v_definition.canonical_spec_text IS NULL
       OR v_definition.canonical_spec_hash IS NULL OR v_definition.canonical_spec_hash IS DISTINCT FROM
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(v_definition.canonical_spec_text, 'UTF8')), 'hex') THEN
        RAISE EXCEPTION 'Periodic definition requires an SDK canonical spec and matching byte hash';
    END IF;
    v_document := v_definition.canonical_spec_text::json;
    -- jsonb discards duplicate keys. Reject ambiguous source JSON before that
    -- conversion, at every nesting level, without inventing another serializer.
    WITH RECURSIVE nodes(value, depth) AS (
        SELECT v_document, 0
        UNION ALL
        SELECT child.value, nodes.depth + 1 FROM nodes
        CROSS JOIN LATERAL (
            SELECT object_item.value FROM pg_catalog.json_each(CASE WHEN pg_catalog.json_typeof(nodes.value) = 'object'
                THEN nodes.value ELSE '{}'::json END) object_item
            UNION ALL
            SELECT array_item.value FROM pg_catalog.json_array_elements(CASE WHEN pg_catalog.json_typeof(nodes.value) = 'array'
                THEN nodes.value ELSE '[]'::json END) array_item
        ) child WHERE nodes.depth < 65
    )
    SELECT COALESCE(bool_or(depth > 64 OR EXISTS (
        SELECT 1 FROM pg_catalog.json_each(CASE WHEN pg_catalog.json_typeof(nodes.value) = 'object'
            THEN nodes.value ELSE '{}'::json END) object_item GROUP BY object_item.key HAVING count(*) > 1
    )), false) INTO v_invalid_document FROM nodes;
    IF v_invalid_document THEN
        RAISE EXCEPTION 'Periodic spec contains duplicate keys or excessive nesting';
    END IF;
    v_spec := v_document::jsonb;
    IF pg_catalog.jsonb_typeof(v_spec) IS DISTINCT FROM 'object'
       OR v_spec->'v' IS DISTINCT FROM '1'::jsonb
       OR pg_catalog.jsonb_typeof(v_spec->'args') IS DISTINCT FROM 'array'
       OR pg_catalog.jsonb_typeof(v_spec->'kwargs') IS DISTINCT FROM 'object'
       OR pg_catalog.jsonb_typeof(v_spec->'takes_context') IS DISTINCT FROM 'boolean'
       OR pg_catalog.jsonb_typeof(v_spec->'provenance') IS DISTINCT FROM 'object'
       OR pg_catalog.jsonb_typeof(v_spec->'backend_alias') IS DISTINCT FROM 'string'
       OR pg_catalog.btrim(v_spec->>'backend_alias') = ''
       OR pg_catalog.jsonb_typeof(v_spec->'task_path') IS DISTINCT FROM 'string'
       OR pg_catalog.btrim(v_spec->>'task_path') = ''
       OR pg_catalog.jsonb_typeof(v_spec->'queue_name') IS DISTINCT FROM 'string'
       OR pg_catalog.btrim(v_spec->>'queue_name') = ''
       OR v_spec->'run_after' IS DISTINCT FROM 'null'::jsonb
       OR v_spec->>'task_path' IS DISTINCT FROM v_definition.task_path
       OR v_spec->>'queue_name' IS DISTINCT FROM v_definition.queue_name
       OR v_spec->'priority' IS DISTINCT FROM pg_catalog.to_jsonb(v_definition.priority)
       OR v_spec->'concurrency_key' IS DISTINCT FROM COALESCE(pg_catalog.to_jsonb(v_definition.concurrency_key), 'null'::jsonb)
       OR v_spec->'concurrency_limit' IS DISTINCT FROM pg_catalog.to_jsonb(v_definition.concurrency_limit)
       OR v_spec#>'{exec,max_attempts}' IS DISTINCT FROM pg_catalog.to_jsonb(v_definition.max_attempts)
       OR v_definition.max_attempts <= 0 OR v_definition.concurrency_limit < 0
       OR v_definition.payload_json IS DISTINCT FROM pg_catalog.jsonb_build_object('args', v_spec->'args', 'kwargs', v_spec->'kwargs')
       OR pg_catalog.jsonb_typeof(v_spec#>'{exec,timeout_seconds}') IS DISTINCT FROM 'number'
       OR (pg_catalog.jsonb_typeof(v_spec->'lock_key') IS DISTINCT FROM 'string' AND v_spec->'lock_key' IS DISTINCT FROM 'null'::jsonb) THEN
        RAISE EXCEPTION 'Periodic definition projections disagree with its immutable SDK spec';
    END IF;
    v_timeout := (v_spec#>>'{exec,timeout_seconds}')::integer;
    IF v_timeout <= 0 THEN
        RAISE EXCEPTION 'Periodic task timeout must be positive';
    END IF;
    INSERT INTO public.task_runs (
        backend_alias, queue_name, priority, run_after, spec_json, spec_hash, canonical_spec_text, spec_version,
        task_path, max_attempts, timeout_seconds, lock_key, concurrency_key, concurrency_limit,
        periodic_name, scheduled_for, periodic_provider
    ) VALUES (
        v_spec->>'backend_alias', v_definition.queue_name, v_definition.priority, p_scheduled_for, v_spec,
        v_definition.canonical_spec_hash, v_definition.canonical_spec_text, 1,
        v_definition.task_path, v_definition.max_attempts, v_timeout, v_spec->>'lock_key',
        v_definition.concurrency_key, v_definition.concurrency_limit, p_name, p_scheduled_for, p_provider
    ) ON CONFLICT (periodic_name, scheduled_for) WHERE periodic_name IS NOT NULL DO NOTHING
    RETURNING task_runs.result_id INTO v_result_id;
    IF FOUND THEN
        UPDATE public.periodic_tasks SET last_run_at = CASE WHEN last_run_at IS NULL OR last_run_at < p_scheduled_for
            THEN p_scheduled_for ELSE last_run_at END, updated_at = pg_catalog.clock_timestamp() WHERE name = p_name;
        RETURN QUERY SELECT v_result_id, true;
        RETURN;
    END IF;
    SELECT tr.result_id INTO v_result_id FROM public.task_runs tr
        WHERE tr.periodic_name = p_name AND tr.scheduled_for = p_scheduled_for;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Concurrent periodic occurrence requires transaction retry' USING ERRCODE = '40001';
    END IF;
    RETURN QUERY SELECT v_result_id, false;
END;
$reproq_occurrence_v1$;

COMMENT ON FUNCTION public.reproq_enqueue_periodic_occurrence(text, timestamp with time zone, text)
IS 'Immutable periodic-enqueue-v1 primitive owned by reproq_django.0003_periodic_enqueue_v1';
