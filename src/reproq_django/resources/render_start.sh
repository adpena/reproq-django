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
prestart_cmd="${REPROQ_PRESTART_CMD:-}"
prestart_interval="${REPROQ_PRESTART_INTERVAL_SECONDS:-5}"
prestart_max_wait="${REPROQ_PRESTART_MAX_WAIT_SECONDS:-120}"
worker_cmd="${REPROQ_WORKER_CMD:-}"
default_beat=0
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
  beat_cmd=""
  default_beat=1
fi
web_cmd="${REPROQ_WEB_CMD:-}"

if [[ -n "$scheduler_mode_normalized" && "$scheduler_mode_normalized" != "beat" ]]; then
  log "REPROQ_SCHEDULER_MODE=${REPROQ_SCHEDULER_MODE}; disabling reproq-beat."
  beat_cmd=""
  default_beat=0
fi

if [[ -z "$web_cmd" ]]; then
  log "REPROQ_WEB_CMD is required (set it to your web process command)."
  exit 1
fi

if [[ -z "$prestart_cmd" || -z "$worker_cmd" || "$default_beat" -eq 1 ]]; then
  # One selected runtime serves all default commands. Login shells may reset
  # PATH, so retain its absolute interpreter rather than another UV parent.
  if [[ -n "${VIRTUAL_ENV:-}" ]]; then
    selected_python="${VIRTUAL_ENV}/bin/python"
  elif ! selected_python="$(uv run python -c 'import sys; print(sys.executable)')"; then
    log "could not resolve the default command interpreter with uv"
    exit 1
  fi
  if [[ "$selected_python" != /* || ! -x "$selected_python" ]]; then
    log "the selected default command interpreter must be an executable absolute path"
    exit 1
  fi
  printf -v python_command '%q' "$selected_python"
  if [[ -z "$prestart_cmd" ]]; then
    prestart_cmd="exec ${python_command} manage.py reproq check"
  fi
  if [[ -z "$worker_cmd" ]]; then
    worker_cmd="exec ${python_command} manage.py reproq worker --concurrency ${REPROQ_CONCURRENCY:-3} --metrics-addr ${METRICS_ADDR:-127.0.0.1:9090}"
  fi
  if [[ "$default_beat" -eq 1 ]]; then
    beat_cmd="exec ${python_command} manage.py reproq beat --interval ${REPROQ_BEAT_INTERVAL:-30s}"
  fi
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
  run_with_restart "reproq-worker" "REPROQ_PROCESS_ROLE=worker ${worker_cmd}" &
  child_pids+=("$!")
else
  log "reproq-worker disabled (empty REPROQ_WORKER_CMD)"
fi

if [[ -n "$beat_cmd" ]]; then
  run_with_restart "reproq-beat" "REPROQ_PROCESS_ROLE=beat ${beat_cmd}" &
  child_pids+=("$!")
else
  log "reproq-beat disabled (empty REPROQ_BEAT_CMD)"
fi

log "web starting"
bash -lc "REPROQ_PROCESS_ROLE=web ${web_cmd}" &
web_pid=$!
child_pids+=("$web_pid")

wait "$web_pid"
web_status=$?

stop_requested=1
shutdown

exit "${web_status}"
