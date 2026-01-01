#!/usr/bin/env bash
set -u
set -o pipefail

stop_requested=0
child_pids=()

log() {
  printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

shutdown() {
  stop_requested=1
  if ((${#child_pids[@]})); then
    log "shutdown requested; stopping child processes"
    for pid in "${child_pids[@]}"; do
      kill -TERM "$pid" 2>/dev/null || true
    done
    wait || true
  fi
}

trap shutdown SIGTERM SIGINT

restart_delay="${REPROQ_RESTART_DELAY_SECONDS:-5}"
prestart_cmd="${REPROQ_PRESTART_CMD:-uv run python manage.py reproq check}"
prestart_interval="${REPROQ_PRESTART_INTERVAL_SECONDS:-5}"
prestart_max_wait="${REPROQ_PRESTART_MAX_WAIT_SECONDS:-120}"
worker_cmd="${REPROQ_WORKER_CMD:-uv run python manage.py reproq worker --concurrency ${REPROQ_CONCURRENCY:-3} --metrics-addr ${METRICS_ADDR:-127.0.0.1:9090}}"
scheduler_mode_normalized="$(printf '%s' "${REPROQ_SCHEDULER_MODE:-}" | tr '[:upper:]' '[:lower:]' | xargs)"
if [[ -v REPROQ_BEAT_CMD ]]; then
  beat_cmd="${REPROQ_BEAT_CMD}"
  beat_cmd_normalized="$(printf '%s' "$beat_cmd" | tr '[:upper:]' '[:lower:]' | xargs)"
  case "$beat_cmd_normalized" in
    ""|"0"|"false"|"off"|"disabled"|"none")
      beat_cmd=""
      ;;
  esac
else
  beat_cmd="uv run python manage.py reproq beat --interval ${REPROQ_BEAT_INTERVAL:-30s}"
fi
web_cmd="${REPROQ_WEB_CMD:-}"

if [[ -n "$scheduler_mode_normalized" && "$scheduler_mode_normalized" != "beat" ]]; then
  log "REPROQ_SCHEDULER_MODE=${REPROQ_SCHEDULER_MODE}; disabling reproq-beat."
  beat_cmd=""
fi

if [[ "${LOW_MEMORY_MODE:-0}" == "1" ]]; then
  log "LOW_MEMORY_MODE enabled; disabling reproq-beat (use pg_cron for schedules)."
  beat_cmd=""
fi

if [[ -z "$web_cmd" ]]; then
  log "REPROQ_WEB_CMD is required (set it to your web process command)."
  exit 1
fi

if [[ -n "$prestart_cmd" ]]; then
  log "prestart: ${prestart_cmd}"
  prestart_started=$(date +%s)
  while true; do
    if bash -lc "$prestart_cmd"; then
      log "prestart ok"
      break
    fi
    if [[ "${stop_requested}" -eq 1 ]]; then
      log "prestart canceled"
      exit 1
    fi
    now=$(date +%s)
    elapsed=$((now - prestart_started))
    if [[ "${prestart_max_wait}" -gt 0 && "${elapsed}" -ge "${prestart_max_wait}" ]]; then
      log "prestart failed after ${elapsed}s"
      exit 1
    fi
    log "prestart failed; retrying in ${prestart_interval}s"
    sleep "${prestart_interval}"
  done
fi

run_with_restart() {
  local name="$1"
  local cmd="$2"
  local cmd_pid=0

  handle_term() {
    stop_requested=1
    if [[ "$cmd_pid" -ne 0 ]]; then
      kill -TERM "$cmd_pid" 2>/dev/null || true
    fi
  }

  trap handle_term SIGTERM SIGINT

  while true; do
    log "${name} starting"
    bash -lc "$cmd" &
    cmd_pid=$!
    wait "$cmd_pid"
    exit_code=$?
    cmd_pid=0
    if [[ "${stop_requested}" -eq 1 ]]; then
      log "${name} stopped"
      break
    fi
    log "${name} exited with ${exit_code}; restarting in ${restart_delay}s"
    sleep "${restart_delay}"
  done
}

if [[ -n "$worker_cmd" ]]; then
  run_with_restart "reproq-worker" "$worker_cmd" &
  child_pids+=("$!")
else
  log "reproq-worker disabled (empty REPROQ_WORKER_CMD)"
fi

if [[ -n "$beat_cmd" ]]; then
  run_with_restart "reproq-beat" "$beat_cmd" &
  child_pids+=("$!")
else
  log "reproq-beat disabled (empty REPROQ_BEAT_CMD)"
fi

log "web starting"
bash -lc "$web_cmd" &
web_pid=$!
child_pids+=("$web_pid")

wait "$web_pid"
web_status=$?

stop_requested=1
shutdown

exit "${web_status}"
