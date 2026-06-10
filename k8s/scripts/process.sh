#!/usr/bin/env bash

# process_cluster_v4_compact.sh
# VERSION: v4-compact-aliases-vm-connector-in-vm
# Run from one machine (for example server2-labo) and collect EcoFloc PID
# measurements from multiple Kubernetes nodes.
#
# It works as a coordinator + remote/local agents:
# - the coordinator runs on the machine where you execute this script;
# - each agent scans local host processes with ps and /proc;
# - remote agents are launched through SSH by streaming this same script to bash;
# - EcoFloc is always executed on the node where the target PID exists.
#
# Default nodes for your current setup:
#   server2-labo -> local, sudo /bin/execute ecofloc
#   fedora       -> ssh kevinoulai@10.0.8.34, sudo ecofloc
#   server1-k3s-worker -> ssh vm, start /home/vagrant/update_freq.sh once, run ecofloc
# SSH sudo is handled by the coordinator before the live display starts, to avoid concurrent prompts.
# VM mode means: connect to the VM SSH alias, start the CPU connector inside the VM, then run EcoFloc inside the VM.
# VM SSH alias expected: ssh vm
# CPU connector inside VM: /home/vagrant/update_freq.sh

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

WATCH_INTERVAL="0.5"
SCAN_INTERVAL="0.1"
ECOFLOC_INTERVAL="1000"
ECOFLOC_METRICS="cpu,ram,sd,nic,gpu"
ECOFLOC_LOG_DIR="/tmp/erctl-ecofloc"
ECOFLOC_EXPORT_PATH=""
FULL=false
WATCH=false
CMD=""

# Default nodes. Override with --node if needed.
# Format accepted by --node:
#   name=local:[execute|direct]
#   name=ssh:user@host:[direct|execute]
#   name=vm:user@host:[execute|direct]
NODES=(
  "server2-labo=local:execute"
  "fedora=ssh:fedora:direct"
  "server1-k3s-worker=vm:vm:direct"
)
CUSTOM_NODES=false
ASK_SSH_SUDO=true

print_help() {
  cat <<'EOF_HELP'
Usage: process.sh [options] [command]

Commands:
  get                       Show Python PIDs running /app/... scripts on all configured nodes.
  ecofloc                   Monitor detected Python PIDs with EcoFloc on all configured nodes.

Options:
  --watch                   Refresh continuously.
  --full                    In get mode, show full commands.
  --interval SECONDS        Table refresh interval. Default: 0.5.
  --scan-interval SECONDS   PID detection interval on agents. Default: 0.1.

Node options:
  --node SPEC               Add a node and disable defaults.
                            Formats:
                              server2-labo=local:execute
                              fedora=ssh:fedora:direct
                              server1-k3s-worker=vm:vm:direct

EcoFloc options:
  --ecofloc-interval MS     EcoFloc measurement interval in ms. Default: 1000.
  --metrics LIST            Comma-separated metrics. Default: cpu,ram,sd,nic,gpu.
  --export PATH             Pass -f PATH to EcoFloc on each node.
                            If PATH contains {node}, it is replaced by node name.

Sudo modes:
  execute                   sudo -n /bin/execute ecofloc ...
  direct                    sudo -n /usr/local/bin/ecofloc ...

Examples:
  ./process.sh --watch ecofloc
  ./process.sh --watch ecofloc --metrics cpu,ram
  ./process.sh get

  ./process.sh --watch ecofloc \
    --node server2-labo=local:execute \
    --node fedora=ssh:fedora:direct \
    --node server1-k3s-worker=vm:vm:direct
EOF_HELP
}

hide_cursor() { tput civis 2>/dev/null || true; }
show_cursor() { tput cnorm 2>/dev/null || true; }
restore_terminal() { show_cursor; stty sane 2>/dev/null || true; }
screen_init() { hide_cursor; printf '\033[2J\033[H'; }
render_file_in_place() { local file="$1"; printf '\033[H'; cat "$file"; printf '\033[J'; }

# -----------------------------------------------------------------------------
# Shared process parsing
# -----------------------------------------------------------------------------

filter_processes() {
  python3 -c '
import os
import sys

SELF_PATTERNS = [
    "sudo -n /bin/execute ecofloc",
    "sudo /bin/execute ecofloc",
    "sudo -n ecofloc",
    "sudo ecofloc",
    "/bin/execute ecofloc",
    "/opt/ecofloc/ecofloc",
    " ecofloc ",
    "process.sh",
    "process_cluster.sh",
    "process_multinode.sh",
    "erctl process",
]

IGNORED_PATTERNS = [
    "argoexec init",
    "argoexec wait",
    "argoexec emissary",
    "python -u -m sidecar",
    "/usr/bin/python3 -sP /usr/bin/firewalld",
    "/usr/bin/python3 -Es /usr/sbin/tuned",
    "/usr/bin/python3 -Es /usr/sbin/tuned-ppd",
]

def parse_line(line):
    line = line.rstrip("\n")
    if not line.strip():
        return None, ""
    parts = line.strip().split(None, 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]

def base(path):
    return os.path.basename(path.rstrip())

def tokens(cmd):
    return cmd.split()

def is_python_token(tok):
    b = base(tok)
    return (
        b == "python"
        or b == "python3"
        or b.startswith("python3.")
        or tok.endswith("/bin/python")
        or tok.endswith("/bin/python3")
        or tok.endswith("/python")
        or "/python" in tok
    )

def should_ignore(cmd):
    return any(p in cmd for p in SELF_PATTERNS) or any(p in cmd for p in IGNORED_PATTERNS)

def is_target_python_process(cmd):
    if should_ignore(cmd):
        return False
    ts = tokens(cmd)
    if len(ts) < 2:
        return False
    return is_python_token(ts[0]) and ts[1].startswith("/app/")

seen = set()
for line in sys.stdin:
    pid, cmd = parse_line(line)
    if not pid or not cmd:
        continue
    if pid in seen:
        continue
    if not is_target_python_process(cmd):
        continue
    seen.add(pid)
    print(f"{pid}\t{cmd}")
'
}

# -----------------------------------------------------------------------------
# Agent mode: runs locally on each node. Emits events to stdout.
# -----------------------------------------------------------------------------

agent_node_name=""
agent_sudo_mode="execute"
AGENT_ECOFLOC_DIRECT_BIN="${ERCTL_ECOFLOC_DIRECT_BIN:-/usr/local/bin/ecofloc}"
AGENT_SUDO_PASSWORD="${ERCTL_AGENT_SUDO_PASSWORD:-}"
if [ -n "${ERCTL_AGENT_SUDO_PASSWORD_B64:-}" ]; then
  AGENT_SUDO_PASSWORD="$(printf "%s" "$ERCTL_AGENT_SUDO_PASSWORD_B64" | base64 -d 2>/dev/null || true)"
fi
AGENT_ROWS_FILE=""
AGENT_SESSIONS_FILE=""
AGENT_RESULTS_FILE=""
AGENT_SUDO_KEEPALIVE_PID=""
AGENT_CLEANUP_DONE=false
AGENT_CPU_CONNECTOR_PID=""
AGENT_CPU_CONNECTOR_STARTED=false
VM_MODE=false
AGENT_VM_CONNECTOR_SCRIPT="${ERCTL_VM_CONNECTOR_SCRIPT:-/home/vagrant/update_freq.sh}"

agent_ecofloc_cmd_prefix() {
  case "$agent_sudo_mode" in
    execute) printf '%s\0%s\0%s\0' "sudo" "-n" "/bin/execute" ;;
    direct)  printf '%s\0%s\0' "sudo" "-n" ;;
    *) echo "[agent:$agent_node_name] invalid sudo mode: $agent_sudo_mode" >&2; exit 1 ;;
  esac
}

agent_build_ecofloc_command() {
  local -n out_ref=$1
  out_ref=()
  if [ "$agent_sudo_mode" = "execute" ]; then
    if [ -n "$AGENT_SUDO_PASSWORD" ]; then
      out_ref=(sudo -S -p "" /bin/execute ecofloc)
    else
      out_ref=(sudo -n /bin/execute ecofloc)
    fi
  else
    # Fedora/direct mode: use the absolute path so a sudoers rule like
    #   kevinoulai ALL=(root) NOPASSWD: /usr/local/bin/ecofloc
    # matches exactly.
    out_ref=(sudo -n "$AGENT_ECOFLOC_DIRECT_BIN")
  fi
}

agent_sudo_validate() {
  # In direct mode we cannot use "ecofloc --help" because EcoFloc may not
  # support that option. The real validation is done once in
  # agent_start_sudo_keepalive() with a short PID measurement.
  # Per-session failures will be captured in the EcoFloc log.
  if [ "$agent_sudo_mode" = "direct" ]; then
    return 0
  fi

  if [ -n "$AGENT_SUDO_PASSWORD" ]; then
    printf '%s
' "$AGENT_SUDO_PASSWORD" | sudo -S -p "" -v >/dev/null 2>&1
  else
    sudo -n -v >/dev/null 2>&1
  fi
}

agent_run_ecofloc_foreground() {
  local cmd=()
  agent_build_ecofloc_command cmd
  if [ -n "$AGENT_SUDO_PASSWORD" ]; then
    printf '%s
' "$AGENT_SUDO_PASSWORD" | "${cmd[@]}" "$@"
  else
    "${cmd[@]}" "$@"
  fi
}

agent_start_ecofloc_background() {
  local log_file="$1"
  shift
  local cmd=()
  agent_build_ecofloc_command cmd
  if [ -n "$AGENT_SUDO_PASSWORD" ]; then
    ( printf '%s
' "$AGENT_SUDO_PASSWORD" | "${cmd[@]}" "$@" ) > "$log_file" 2>&1 &
  else
    "${cmd[@]}" "$@" > "$log_file" 2>&1 &
  fi
  echo "$!"
}

agent_pid_exists() {
  local pid="$1"
  [ -d "/proc/$pid" ] || ps -p "$pid" >/dev/null 2>&1
}

agent_children_of() { pgrep -P "$1" 2>/dev/null || true; }

agent_all_descendants() {
  local parent="$1"
  local child=""
  for child in $(agent_children_of "$parent"); do
    echo "$child"
    agent_all_descendants "$child"
  done
}

agent_stop_process_tree_with_int() {
  local root_pid="$1"
  local descendants=""
  [ -n "${root_pid:-}" ] || return 0
  descendants="$(agent_all_descendants "$root_pid" | tac 2>/dev/null || agent_all_descendants "$root_pid")"
  for p in $descendants; do kill -INT "$p" 2>/dev/null || true; done
  kill -INT "$root_pid" 2>/dev/null || true
  sleep 1
  for p in $descendants; do ps -p "$p" >/dev/null 2>&1 && kill -TERM "$p" 2>/dev/null || true; done
  ps -p "$root_pid" >/dev/null 2>&1 && kill -TERM "$root_pid" 2>/dev/null || true
  sleep 0.5
  for p in $descendants; do ps -p "$p" >/dev/null 2>&1 && kill -KILL "$p" 2>/dev/null || true; done
  ps -p "$root_pid" >/dev/null 2>&1 && kill -KILL "$root_pid" 2>/dev/null || true
}

agent_emit() {
  # Keep lines short enough for atomic pipe writes.
  printf '%s\n' "$*"
}

agent_get_raw_processes() { ps axww -o pid=,args=; }
agent_get_matching_processes() { agent_get_raw_processes | filter_processes; }

agent_start_sudo_keepalive() {
  echo "[agent:$agent_node_name] checking sudo access for EcoFloc PID mode..." >&2
  if ! agent_sudo_validate; then
    echo "[agent:$agent_node_name] sudo validation failed." >&2
    exit 1
  fi

  local test_pid=""
  local test_log="/tmp/erctl-ecofloc-sudo-test-${agent_node_name}.log"

  sleep 10 &
  test_pid="$!"

  if ! agent_run_ecofloc_foreground --cpu -p "$test_pid" -i 1000 -t 1 > "$test_log" 2>&1; then
    kill "$test_pid" 2>/dev/null || true
    echo "[agent:$agent_node_name] cannot run EcoFloc PID mode with sudo mode '$agent_sudo_mode'." >&2
    tail -n 20 "$test_log" >&2 || true
    exit 1
  fi

  kill "$test_pid" 2>/dev/null || true

  if [ "$agent_sudo_mode" = "execute" ]; then
    (
      while true; do
        agent_sudo_validate || exit 0
        sleep 60
      done
    ) &
    AGENT_SUDO_KEEPALIVE_PID="$!"
  fi
}

agent_stop_sudo_keepalive() {
  if [ -n "${AGENT_SUDO_KEEPALIVE_PID:-}" ]; then
    kill "$AGENT_SUDO_KEEPALIVE_PID" 2>/dev/null || true
    wait "$AGENT_SUDO_KEEPALIVE_PID" 2>/dev/null || true
    AGENT_SUDO_KEEPALIVE_PID=""
  fi
}

agent_stop_all_ecofloc_sessions() {
  [ -n "${AGENT_SESSIONS_FILE:-}" ] || return 0
  [ -f "$AGENT_SESSIONS_FILE" ] || return 0
  local pid="" metric="" eco_pid="" log_file="" start_ts=""
  while IFS=$'\t' read -r pid metric eco_pid log_file start_ts; do
    [ -n "${eco_pid:-}" ] || continue
    agent_stop_process_tree_with_int "$eco_pid"
  done < "$AGENT_SESSIONS_FILE"
}

agent_cleanup() {
  local exit_code="${1:-0}"
  if [ "$AGENT_CLEANUP_DONE" = true ]; then
    exit "$exit_code"
  fi
  AGENT_CLEANUP_DONE=true
  agent_stop_all_ecofloc_sessions >/dev/null 2>&1 || true
  agent_stop_cpu_connector >/dev/null 2>&1 || true
  agent_stop_sudo_keepalive >/dev/null 2>&1 || true
  [ -n "${AGENT_ROWS_FILE:-}" ] && rm -f "$AGENT_ROWS_FILE" 2>/dev/null || true
  [ -n "${AGENT_SESSIONS_FILE:-}" ] && rm -f "$AGENT_SESSIONS_FILE" 2>/dev/null || true
  [ -n "${AGENT_RESULTS_FILE:-}" ] && rm -f "$AGENT_RESULTS_FILE" 2>/dev/null || true
  exit "$exit_code"
}

agent_append_row_if_missing() {
  local pid="$1"
  local cmd="$2"
  python3 - "$AGENT_ROWS_FILE" "$agent_node_name" "$pid" "$cmd" <<'PY'
import sys
rows_file, node, pid, cmd = sys.argv[1:5]
try:
    rows = open(rows_file, encoding="utf-8").read().splitlines()
except FileNotFoundError:
    rows = []
for row in rows:
    parts = row.split("\t", 2)
    if len(parts) >= 2 and parts[0] == node and parts[1] == pid:
        sys.exit(0)
rows.append(f"{node}\t{pid}\t{cmd}")
with open(rows_file, "w", encoding="utf-8") as f:
    f.write("\n".join(rows) + "\n")
PY
  agent_emit $'ROW\t'"${agent_node_name}"$'\t'"${pid}"$'\t'"${cmd}"
}

agent_session_exists() { grep -q "^${1}"$'\t'"${2}"$'\t' "$AGENT_SESSIONS_FILE" 2>/dev/null; }
agent_result_exists() { grep -q "^${1}"$'\t'"${2}"$'\t' "$AGENT_RESULTS_FILE" 2>/dev/null; }

agent_write_result() {
  local pid="$1" metric="$2" value="$3" tmp=""
  tmp="$(mktemp)"
  if [ -f "$AGENT_RESULTS_FILE" ]; then
    grep -v "^${pid}"$'\t'"${metric}"$'\t' "$AGENT_RESULTS_FILE" > "$tmp" || true
  fi
  printf "%s\t%s\t%s\n" "$pid" "$metric" "$value" >> "$tmp"
  mv "$tmp" "$AGENT_RESULTS_FILE"
  agent_emit $'RESULT\t'"${agent_node_name}"$'\t'"${pid}"$'\t'"${metric}"$'\t'"${value}"
}

agent_start_ecofloc_session() {
  local pid="$1" metric="$2" log_file="$3"
  local export_args=()
  local cmd=()

  mkdir -p "$ECOFLOC_LOG_DIR"

  if [ -n "$ECOFLOC_EXPORT_PATH" ]; then
    local export_path="$ECOFLOC_EXPORT_PATH"
    export_path="${export_path//\{node\}/$agent_node_name}"
    export_args=(-f "$export_path")
  fi

  agent_start_ecofloc_background "$log_file" "--${metric}" \
    -p "$pid" \
    -i "$ECOFLOC_INTERVAL" \
    -t -1 \
    "${export_args[@]}"
}

agent_parse_ecofloc_log() {
  local log_file="$1"
  if [ ! -f "$log_file" ]; then echo "NO_LOG"; return 0; fi

  local avg="" total=""
  avg="$(awk -F ':' 'BEGIN { IGNORECASE = 1 } /Average[[:space:]]+Power/ { gsub(/^[ \t]+|[ \t]+$/, "", $2); last = $2 } END { if (last != "") print last }' "$log_file" | awk '{ print $1 }')"
  total="$(awk -F ':' 'BEGIN { IGNORECASE = 1 } /Total.*Energy/ { gsub(/^[ \t]+|[ \t]+$/, "", $2); last = $2 } END { if (last != "") print last }' "$log_file" | awk '{ print $1 }')"

  if [ -n "$avg" ] || [ -n "$total" ]; then
    [ -z "$avg" ] && avg="?"
    [ -z "$total" ] && total="?"
    echo "${avg}W/${total}J"
    return 0
  fi
  if grep -qi "a password is required\|a terminal is required\|no tty\|sudo:" "$log_file"; then echo "SUDO_AUTH"; return 0; fi
  if grep -qi "invalid option\|usage:" "$log_file"; then echo "BAD_ARGS"; return 0; fi
  if grep -qi "not allowed\|not permitted\|permission denied" "$log_file"; then echo "DENIED"; return 0; fi
  if grep -qi "no such process\|invalid pid\|process.*not.*found" "$log_file"; then echo "BAD_PID"; return 0; fi
  local last_line=""
  last_line="$(grep -v '^[[:space:]]*$' "$log_file" | tail -n 1 | tr -d '\r' | cut -c 1-14)"
  [ -n "$last_line" ] && echo "LOG:${last_line}" || echo "EMPTY_LOG"
}

agent_create_sessions_for_matches() {
  local matches="$1"
  local pid="" cmd="" metric="" log_file="" eco_pid="" start_ts=""
  while IFS=$'\t' read -r pid cmd; do
    [ -n "${pid:-}" ] || continue
    [ -n "${cmd:-}" ] || continue
    agent_append_row_if_missing "$pid" "$cmd"
    IFS=',' read -r -a metric_list <<< "$ECOFLOC_METRICS"
    for metric in "${metric_list[@]}"; do
      metric="$(printf "%s" "$metric" | tr '[:upper:]' '[:lower:]' | xargs)"
      case "$metric" in
        cpu|ram|sd|nic|gpu)
          if agent_result_exists "$pid" "$metric" || agent_session_exists "$pid" "$metric"; then continue; fi
          if ! agent_pid_exists "$pid"; then agent_write_result "$pid" "$metric" "TOO_SHORT"; continue; fi
          if ! agent_sudo_validate; then agent_write_result "$pid" "$metric" "SUDO_AUTH"; continue; fi
          start_ts="$(date +%s)"
          log_file="${ECOFLOC_LOG_DIR}/ecofloc_${agent_node_name}_${pid}_${metric}_${start_ts}.log"
          eco_pid="$(agent_start_ecofloc_session "$pid" "$metric" "$log_file")"
          printf "%s\t%s\t%s\t%s\t%s\n" "$pid" "$metric" "$eco_pid" "$log_file" "$start_ts" >> "$AGENT_SESSIONS_FILE"
          agent_emit $'ACTIVE\t'"${agent_node_name}"$'\t'"${pid}"$'\t'"${metric}"
          sleep 0.03
          ;;
      esac
    done
  done <<< "$matches"
}

agent_finalize_finished_sessions() {
  [ -f "$AGENT_SESSIONS_FILE" ] || return 0
  local tmp="" pid="" metric="" eco_pid="" log_file="" start_ts="" now="" elapsed="" value=""
  tmp="$(mktemp)"
  now="$(date +%s)"
  while IFS=$'\t' read -r pid metric eco_pid log_file start_ts; do
    [ -n "${pid:-}" ] || continue
    [ -n "${metric:-}" ] || continue
    [ -n "${eco_pid:-}" ] || continue
    [ -n "${log_file:-}" ] || continue
    [ -n "${start_ts:-}" ] || start_ts="$now"
    if agent_pid_exists "$pid"; then
      printf "%s\t%s\t%s\t%s\t%s\n" "$pid" "$metric" "$eco_pid" "$log_file" "$start_ts" >> "$tmp"
      continue
    fi
    elapsed="$((now - start_ts))"
    agent_stop_process_tree_with_int "$eco_pid"
    value="$(agent_parse_ecofloc_log "$log_file" | tail -n 1 | tr -d '\r')"
    if [ -z "$value" ] || [ "$value" = "NO_LOG" ] || [ "$value" = "EMPTY_LOG" ]; then
      if [ "$elapsed" -lt 1 ]; then value="TOO_SHORT"; else value="NO_DATA"; fi
    fi
    agent_write_result "$pid" "$metric" "$value"
  done < "$AGENT_SESSIONS_FILE"
  mv "$tmp" "$AGENT_SESSIONS_FILE"
}

agent_start_cpu_connector(){
  local connector_script="${AGENT_VM_CONNECTOR_SCRIPT:-/home/vagrant/update_freq.sh}"
  local lock_file="/tmp/erctl-vm-connector.lock"
  local pid_file="/tmp/erctl-vm-connector.pid"

  if pgrep -f "${connector_script}" >/dev/null 2>&1; then
    echo "[agent:$agent_node_name] CPU connector already running: ${connector_script}" >&2
    AGENT_CPU_CONNECTOR_STARTED=false
    return 0
  fi

  (
    flock -n 9 || exit 0

    if pgrep -f "${connector_script}" >/dev/null 2>&1; then
      exit 0
    fi

    if [ ! -f "$connector_script" ]; then
      echo "[agent:$agent_node_name] CPU connector not found: ${connector_script}" >&2
      exit 2
    fi

    echo "[agent:$agent_node_name] starting CPU connector in VM: ${connector_script}" >&2
    sh "$connector_script" >/tmp/erctl-vm-connector.log 2>&1 &
    echo "$!" > "$pid_file"
  ) 9>"$lock_file"

  if [ -f "$pid_file" ]; then
    AGENT_CPU_CONNECTOR_PID="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -n "$AGENT_CPU_CONNECTOR_PID" ] && ps -p "$AGENT_CPU_CONNECTOR_PID" >/dev/null 2>&1; then
      AGENT_CPU_CONNECTOR_STARTED=true
    fi
  fi
}

agent_stop_cpu_connector() {
  if [ "$AGENT_CPU_CONNECTOR_STARTED" = true ] && [ -n "${AGENT_CPU_CONNECTOR_PID:-}" ]; then
    echo "[agent:$agent_node_name] stopping CPU connector: ${AGENT_CPU_CONNECTOR_PID}" >&2
    kill "$AGENT_CPU_CONNECTOR_PID" 2>/dev/null || true
    wait "$AGENT_CPU_CONNECTOR_PID" 2>/dev/null || true
    rm -f /tmp/erctl-vm-connector.pid 2>/dev/null || true
    AGENT_CPU_CONNECTOR_PID=""
    AGENT_CPU_CONNECTOR_STARTED=false
  fi
}

agent_main() {
  shift # __agent__
  local mode="watch"
  while [ $# -gt 0 ]; do
    case "$1" in
      --node-name) agent_node_name="$2"; shift 2 ;;
      --sudo-mode) agent_sudo_mode="$2"; shift 2 ;;
      --mode) mode="$2"; shift 2 ;;
      --scan-interval) SCAN_INTERVAL="$2"; shift 2 ;;
      --ecofloc-interval) ECOFLOC_INTERVAL="$2"; shift 2 ;;
      --metrics) ECOFLOC_METRICS="$2"; shift 2 ;;
      --export) ECOFLOC_EXPORT_PATH="$2"; shift 2 ;;
      --vm) VM_MODE=true; shift ;;
      --vm-connector-script) AGENT_VM_CONNECTOR_SCRIPT="$2"; shift 2 ;;
      *) shift ;;
    esac
  done

  [ -n "$agent_node_name" ] || agent_node_name="$(hostname)"

  AGENT_ROWS_FILE="$(mktemp)"
  AGENT_SESSIONS_FILE="$(mktemp)"
  AGENT_RESULTS_FILE="$(mktemp)"
  trap 'agent_cleanup 0' EXIT
  trap 'agent_cleanup 130' INT TERM HUP QUIT TSTP

  if [ "$mode" = "get" ]; then
    local matches=""
    matches="$(agent_get_matching_processes || true)"
    while IFS=$'\t' read -r pid cmd; do
      [ -n "${pid:-}" ] || continue
      agent_emit $'ROW\t'"${agent_node_name}"$'\t'"${pid}"$'\t'"${cmd}"
    done <<< "$matches"
    agent_cleanup 0
  fi

  agent_start_sudo_keepalive
  if [ "$VM_MODE" = "true" ]; then
    agent_start_cpu_connector
  fi
  mkdir -p "$ECOFLOC_LOG_DIR"

  while true; do
    local matches=""
    matches="$(agent_get_matching_processes || true)"
    if [ -n "$matches" ]; then agent_create_sessions_for_matches "$matches"; fi
    agent_finalize_finished_sessions
    local active="0"
    active="$(wc -l < "$AGENT_SESSIONS_FILE" | xargs)"
    agent_emit $'HEARTBEAT\t'"${agent_node_name}"$'\t'"${active}"
    sleep "$SCAN_INTERVAL"
  done
}

# -----------------------------------------------------------------------------
# Coordinator mode
# -----------------------------------------------------------------------------

COORD_ROWS_FILE=""
COORD_RESULTS_FILE=""
COORD_ACTIVE_FILE=""
COORD_HEARTBEAT_FILE=""
COORD_PIDS=()
COORD_FIFO=""
COORD_DONE=false
COORD_LOG_DIR="/tmp/process-cluster-logs"

coord_cleanup() {
  local code="${1:-0}"
  if [ "$COORD_DONE" = true ]; then exit "$code"; fi
  COORD_DONE=true
  for p in "${COORD_PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  for p in "${COORD_PIDS[@]:-}"; do wait "$p" 2>/dev/null || true; done
  restore_terminal
  [ -n "${COORD_ROWS_FILE:-}" ] && rm -f "$COORD_ROWS_FILE" 2>/dev/null || true
  [ -n "${COORD_RESULTS_FILE:-}" ] && rm -f "$COORD_RESULTS_FILE" 2>/dev/null || true
  [ -n "${COORD_ACTIVE_FILE:-}" ] && rm -f "$COORD_ACTIVE_FILE" 2>/dev/null || true
  [ -n "${COORD_HEARTBEAT_FILE:-}" ] && rm -f "$COORD_HEARTBEAT_FILE" 2>/dev/null || true
  [ -n "${COORD_FIFO:-}" ] && rm -f "$COORD_FIFO" 2>/dev/null || true
  exit "$code"
}

parse_node_spec() {
  # Echoes: name|kind|target|sudo_mode
  # Supported:
  #   server2-labo=local:execute
  #   fedora=ssh:kevinoulai@10.0.8.34:direct
  local spec="${1:-}"
  local parsed_name="" parsed_rest="" parsed_kind="" parsed_target="" parsed_sudo_mode=""

  if [ -z "$spec" ]; then
    echo "Invalid empty node spec" >&2
    exit 1
  fi

  if [[ "$spec" != *=* ]]; then
    echo "Invalid node spec, missing '=': $spec" >&2
    exit 1
  fi

  parsed_name="${spec%%=*}"
  parsed_rest="${spec#*=}"

  if [[ "$parsed_rest" != *:* ]]; then
    echo "Invalid node spec, missing kind/sudo separator ':': $spec" >&2
    exit 1
  fi

  parsed_kind="${parsed_rest%%:*}"
  parsed_rest="${parsed_rest#*:}"

  case "$parsed_kind" in
    local)
      parsed_target=""
      parsed_sudo_mode="$parsed_rest"
      ;;
    ssh)
      if [[ "$parsed_rest" != *:* ]]; then
        echo "Invalid SSH node spec, expected name=ssh:user@host:mode: $spec" >&2
        exit 1
      fi
      parsed_target="${parsed_rest%%:*}"
      parsed_sudo_mode="${parsed_rest##*:}"
      ;;
    vm)
      if [[ "$parsed_rest" != *:* ]]; then
        echo "Invalid VM node spec, expected name=vm:user@host:mode: $spec" >&2
        exit 1
      fi
      parsed_target="${parsed_rest%%:*}"
      parsed_sudo_mode="${parsed_rest##*:}"
      ;;
    *)
      echo "Invalid node kind in spec: $spec" >&2
      exit 1
      ;;
  esac

  if [ -z "$parsed_name" ] || [ -z "$parsed_kind" ] || [ -z "$parsed_sudo_mode" ]; then
    echo "Invalid node spec after parsing: $spec" >&2
    exit 1
  fi

  if [ "$parsed_kind" = "ssh" ] || [ "$parsed_kind" = "vm" ]; then
    if [ -z "$parsed_target" ]; then
      echo "Invalid $parsed_kind node spec, missing target: $spec" >&2
      exit 1
    fi
  fi

  case "$parsed_sudo_mode" in
    execute|direct) ;;
    *) echo "Invalid sudo mode '$parsed_sudo_mode' in spec: $spec" >&2; exit 1 ;;
  esac

  printf '%s|%s|%s|%s\n' "$parsed_name" "$parsed_kind" "$parsed_target" "$parsed_sudo_mode"
}

start_agent_for_node() {
  local spec="$1"
  local mode="$2"
  local parsed="" name="" kind="" target="" sudo_mode=""
  local agent_err=""

  parsed="$(parse_node_spec "$spec")"
  IFS='|' read -r name kind target sudo_mode <<< "$parsed"

  mkdir -p "$COORD_LOG_DIR"
  agent_err="$COORD_LOG_DIR/agent-${name}.err"

  echo "[coordinator] starting agent: node=$name kind=$kind sudo=$sudo_mode target=${target:-local}" >&2

  if [ "$kind" = "local" ]; then
    if [ "$mode" != "get" ]; then
      echo "[coordinator] validating local sudo for $name..." >&2
      sudo -v
    fi

    bash "$0" __agent__ \
      --node-name "$name" \
      --sudo-mode "$sudo_mode" \
      --mode "$mode" \
      --scan-interval "$SCAN_INTERVAL" \
      --ecofloc-interval "$ECOFLOC_INTERVAL" \
      --metrics "$ECOFLOC_METRICS" \
      --export "$ECOFLOC_EXPORT_PATH" > "$COORD_FIFO" 2> "$agent_err" &

    COORD_PIDS+=("$!")
    return 0
  fi

  local remote_script="/tmp/process_cluster_${USER:-user}.sh"
  local sudo_password=""
  local sudo_password_b64=""

  echo "[coordinator] copying agent script to $target:$remote_script" >&2
  if ! scp -q -o ConnectTimeout=5 "$0" "$target:$remote_script" 2> "$agent_err"; then
    echo "[coordinator] failed to copy agent script to $target. See $agent_err" >&2
    return 1
  fi

  if [ "$mode" != "get" ] && [ "$sudo_mode" = "execute" ]; then
    printf "[coordinator] sudo password for %s: " "$target" >&2
    stty -echo 2>/dev/null || true
    IFS= read -r sudo_password
    stty echo 2>/dev/null || true
    printf "\n" >&2

    if ! printf '%s\n' "$sudo_password" | ssh -o ConnectTimeout=5 "$target" "sudo -S -p '' -v" >/dev/null 2> "$agent_err"; then
      echo "[coordinator] remote sudo validation failed for $target. See $agent_err" >&2
      return 1
    fi

    sudo_password_b64="$(printf '%s' "$sudo_password" | base64 | tr -d '\n')"
    unset sudo_password
  elif [ "$mode" != "get" ] && [ "$sudo_mode" = "direct" ]; then
    echo "[coordinator] validating remote direct sudo for $target without password..." >&2
    if ! ssh -o ConnectTimeout=5 "$target" 'tmp_log="/tmp/erctl-ecofloc-remote-sudo-test.log"; sleep 10 & test_pid=$!; sudo -n /usr/local/bin/ecofloc --cpu -p "$test_pid" -i 1000 -t 1 > "$tmp_log" 2>&1; rc=$?; kill "$test_pid" 2>/dev/null || true; if [ "$rc" -ne 0 ]; then cat "$tmp_log" >&2; fi; exit "$rc"' 2> "$agent_err"; then
      echo "[coordinator] remote direct sudo validation failed for $target. Check sudoers for /usr/local/bin/ecofloc. See $agent_err" >&2
      return 1
    fi
  fi

  local extra_agent_args=()
  if [ "$kind" = "vm" ]; then
    local vm_connector_script="${ERCTL_VM_CONNECTOR_SCRIPT:-/home/vagrant/update_freq.sh}"
    extra_agent_args=(--vm --vm-connector-script "$vm_connector_script")
  fi

  ssh -o ConnectTimeout=5 "$target" \
    "ERCTL_AGENT_SUDO_PASSWORD_B64='$sudo_password_b64' ERCTL_ECOFLOC_DIRECT_BIN='/usr/local/bin/ecofloc' ERCTL_VM_CONNECTOR_SCRIPT='${ERCTL_VM_CONNECTOR_SCRIPT:-/home/vagrant/update_freq.sh}' exec bash '$remote_script' __agent__ --node-name '$name' --sudo-mode '$sudo_mode' --mode '$mode' --scan-interval '$SCAN_INTERVAL' --ecofloc-interval '$ECOFLOC_INTERVAL' --metrics '$ECOFLOC_METRICS' --export '$ECOFLOC_EXPORT_PATH' ${extra_agent_args[*]}" \
    > "$COORD_FIFO" 2> "$agent_err" &

  COORD_PIDS+=("$!")
}

coord_append_row() {
  local node="$1" pid="$2" cmd="$3"
  python3 - "$COORD_ROWS_FILE" "$node" "$pid" "$cmd" <<'PY'
import sys
path,node,pid,cmd=sys.argv[1:5]
try:
    rows=open(path,encoding="utf-8").read().splitlines()
except FileNotFoundError:
    rows=[]
for r in rows:
    parts=r.split("\t",2)
    if len(parts)>=2 and parts[0]==node and parts[1]==pid:
        sys.exit(0)
rows.append(f"{node}\t{pid}\t{cmd}")
open(path,"w",encoding="utf-8").write("\n".join(rows)+"\n")
PY
}

coord_set_result() {
  local node="$1" pid="$2" metric="$3" value="$4" tmp=""
  tmp="$(mktemp)"
  [ -f "$COORD_RESULTS_FILE" ] && grep -v "^${node}"$'\t'"${pid}"$'\t'"${metric}"$'\t' "$COORD_RESULTS_FILE" > "$tmp" || true
  printf "%s\t%s\t%s\t%s\n" "$node" "$pid" "$metric" "$value" >> "$tmp"
  mv "$tmp" "$COORD_RESULTS_FILE"
  # A final result means the active session is over.
  coord_remove_active "$node" "$pid" "$metric"
}

coord_add_active() {
  local node="$1" pid="$2" metric="$3"
  if ! grep -q "^${node}"$'\t'"${pid}"$'\t'"${metric}"'$' "$COORD_ACTIVE_FILE" 2>/dev/null; then
    printf "%s\t%s\t%s\n" "$node" "$pid" "$metric" >> "$COORD_ACTIVE_FILE"
  fi
}

coord_remove_active() {
  local node="$1" pid="$2" metric="$3" tmp=""
  tmp="$(mktemp)"
  [ -f "$COORD_ACTIVE_FILE" ] && grep -v "^${node}"$'\t'"${pid}"$'\t'"${metric}"'$' "$COORD_ACTIVE_FILE" > "$tmp" || true
  mv "$tmp" "$COORD_ACTIVE_FILE"
}

coord_set_heartbeat() {
  local node="$1" active="$2" tmp=""
  tmp="$(mktemp)"
  [ -f "$COORD_HEARTBEAT_FILE" ] && grep -v "^${node}"$'\t' "$COORD_HEARTBEAT_FILE" > "$tmp" || true
  printf "%s\t%s\t%s\n" "$node" "$active" "$(date +%s)" >> "$tmp"
  mv "$tmp" "$COORD_HEARTBEAT_FILE"
}

render_table() {
  python3 - "$COORD_ROWS_FILE" "$COORD_ACTIVE_FILE" "$COORD_RESULTS_FILE" "$COORD_HEARTBEAT_FILE" "$ECOFLOC_METRICS" "$FULL" "$CMD" 2>/dev/null <<'PY'
import os, re, sys, time
from collections import OrderedDict
rows_file, active_file, results_file, heartbeat_file, metrics_raw, full_raw, cmd_mode = sys.argv[1:8]
enabled_metrics={m.strip().lower() for m in metrics_raw.split(',') if m.strip()}
METRICS=["cpu","ram","sd","nic","gpu"]
VALUE_RE=re.compile(r"^\s*([0-9.]+|\?)W/([0-9.]+|\?)J\s*$")
NODE_W=12; PIDS_W=12; SCRIPT_W=18; FUNC_W=16; METRIC_W=12; TOTAL_W=10

def fit(v,w):
    v=str(v)
    return v if len(v)<=w else v[:max(w-1,0)]+'…'
def base(p): return os.path.basename(p.rstrip())
def tokens(cmd): return cmd.split()
def clean(v): return v.strip().strip('"').strip("'")
def extract_script(cmd):
    ts=tokens(cmd)
    return base(ts[1]) if len(ts)>=2 and ts[1].startswith('/app/') else '-'
def extract_function(cmd):
    ts=tokens(cmd)
    for i,tok in enumerate(ts):
        if tok=='--function' and i+1<len(ts): return clean(ts[i+1])
        if tok.startswith('--function='): return clean(tok.split('=',1)[1])
    m=re.search(r"--function(?:=|\s+)([^ ]+)",cmd)
    return clean(m.group(1)) if m else '-'
def parse_metric(v):
    m=VALUE_RE.match(str(v))
    if not m: return 0.0,0.0,False
    p,e=m.groups(); return (0.0 if p=='?' else float(p), 0.0 if e=='?' else float(e), True)
def fmt_metric(p,e,c): return '-' if c<=0 else f"{p:.2f}W/{e:.2f}J"
def fmt_energy(e,c): return '-' if c<=0 else f"{e:.2f}J"
def spinner(): return ["|","/","-","\\"][int(time.time()*4)%4]+' measuring'

rows=[]
try: rows=open(rows_file,encoding='utf-8').read().splitlines()
except FileNotFoundError: pass
active=set()
try:
    for line in open(active_file,encoding='utf-8'):
        parts=line.rstrip('\n').split('\t')
        if len(parts)>=3: active.add(tuple(parts[:3]))
except FileNotFoundError: pass
results={}
try:
    for line in open(results_file,encoding='utf-8'):
        parts=line.rstrip('\n').split('\t',3)
        if len(parts)==4:
            node,pid,metric,value=parts
            results[(node,pid,metric)] = value
except FileNotFoundError: pass
heartbeats={}
try:
    for line in open(heartbeat_file,encoding='utf-8'):
        parts=line.rstrip('\n').split('\t')
        if len(parts)>=3: heartbeats[parts[0]]=(parts[1],parts[2])
except FileNotFoundError: pass

print('Cluster EcoFloc PID monitoring. Press Ctrl+C to stop.' if cmd_mode=='ecofloc' else 'Cluster Python PID scan.')
if heartbeats:
    hb=[]
    now=int(time.time())
    for node,(count,ts) in sorted(heartbeats.items()):
        age=now-int(ts)
        hb.append(f"{node}: active={count}, {age}s ago")
    print('Agents: '+ ' | '.join(hb))
print()

groups=OrderedDict()
for r in rows:
    if not r.strip(): continue
    parts=r.split('\t',2)
    if len(parts)!=3: continue
    node,pid,pcmd=parts
    script=extract_script(pcmd); func=extract_function(pcmd)
    key=(node,script,func)
    groups.setdefault(key, {"node":node,"pids":[],"script":script,"function":func,"cmds":[]})
    if pid not in groups[key]['pids']: groups[key]['pids'].append(pid)
    groups[key]['cmds'].append(pcmd)

if full_raw.lower()=='true' and cmd_mode=='get':
    for g in groups.values():
        for pid,pcmd in zip(g['pids'], g['cmds']):
            print(f"{g['node']}\t{pid}\t{pcmd}")
    sys.exit(0)

metric_cols=[m for m in METRICS if m in enabled_metrics]
headers=["NODE","PIDS","SCRIPT","FUNCTION"] + [m.upper() for m in metric_cols] + ["TOTAL"]
widths=[NODE_W,PIDS_W,SCRIPT_W,FUNC_W] + [METRIC_W for _ in metric_cols] + [TOTAL_W]
print(' | '.join(f"{h:<{w}}" for h,w in zip(headers,widths)))
print('-+-'.join('-'*w for w in widths))
col_p={m:0.0 for m in METRICS}; col_e={m:0.0 for m in METRICS}; col_c={m:0 for m in METRICS}; col_a={m:False for m in METRICS}
grand_e=0.0; grand_c=0; grand_a=False

def aggregate(node,pids,metric):
    if metric not in enabled_metrics: return '-',0.0,0.0,0,False
    vals=[]; act=False
    for pid in pids:
        if (node,pid,metric) in active: act=True
        if (node,pid,metric) in results: vals.append(results[(node,pid,metric)])
    tp=te=0.0; vc=0; statuses=[]
    for v in vals:
        p,e,valid=parse_metric(v)
        if valid: tp+=p; te+=e; vc+=1
        else: statuses.append(v)
    if act: disp=spinner()
    elif vc>0: disp=fmt_metric(tp,te,vc)
    elif statuses:
        u=sorted(set(statuses)); disp=u[0] if len(u)==1 else 'PARTIAL'
    else: disp='WAIT' if cmd_mode=='ecofloc' else '-'
    return disp,tp,te,vc,act

for g in groups.values():
    node=g['node']; pids=g['pids']; row_e=0.0; row_c=0; row_a=False; mvals={}
    for m in METRICS:
        disp,p,e,c,a=aggregate(node,pids,m)
        mvals[m]=disp
        if c>0:
            row_e+=e; row_c+=c; col_p[m]+=p; col_e[m]+=e; col_c[m]+=c; grand_e+=e; grand_c+=c
        if a: row_a=True; col_a[m]=True; grand_a=True
    vals=[node, ','.join(pids), g['script'], g['function']] + [mvals[m] for m in metric_cols] + [spinner() if row_a else fmt_energy(row_e,row_c)]
    print(' | '.join(f"{fit(v,w):<{w}}" for v,w in zip(vals,widths)))
print('-+-'.join('-'*w for w in widths))
def ctot(m): return spinner() if col_a[m] else fmt_metric(col_p[m],col_e[m],col_c[m])
vals=['TOTAL','-','-','-'] + [ctot(m) for m in metric_cols] + [spinner() if grand_a else fmt_energy(grand_e,grand_c)]
print(' | '.join(f"{fit(v,w):<{w}}" for v,w in zip(vals,widths)))
PY
}

coord_render_frame() {
  local frame=""
  frame="$(mktemp)"
  render_table > "$frame"
  render_file_in_place "$frame"
  rm -f "$frame"
}

handle_event_line() {
  local line="$1"
  local type="" node="" pid="" metric="" value="" cmd_line="" active=""
  type="${line%%$'\t'*}"
  case "$type" in
    ROW)
      IFS=$'\t' read -r _ node pid cmd_line <<< "$line"
      [ -n "${node:-}" ] && [ -n "${pid:-}" ] && coord_append_row "$node" "$pid" "$cmd_line"
      ;;
    ACTIVE)
      IFS=$'\t' read -r _ node pid metric <<< "$line"
      [ -n "${node:-}" ] && coord_add_active "$node" "$pid" "$metric"
      ;;
    RESULT)
      IFS=$'\t' read -r _ node pid metric value <<< "$line"
      [ -n "${node:-}" ] && coord_set_result "$node" "$pid" "$metric" "$value"
      ;;
    HEARTBEAT)
      IFS=$'\t' read -r _ node active <<< "$line"
      [ -n "${node:-}" ] && coord_set_heartbeat "$node" "$active"
      ;;
  esac
}

coord_main() {
  COORD_ROWS_FILE="$(mktemp)"
  COORD_RESULTS_FILE="$(mktemp)"
  COORD_ACTIVE_FILE="$(mktemp)"
  COORD_HEARTBEAT_FILE="$(mktemp)"
  COORD_FIFO="$(mktemp -u)"
  mkfifo "$COORD_FIFO"

  trap 'coord_cleanup 0' EXIT
  trap 'coord_cleanup 130' INT TERM HUP QUIT TSTP

  local agent_mode="watch"
  [ "$CMD" = "get" ] && agent_mode="get"

  echo "[coordinator] starting with ${#NODES[@]} configured node(s)" >&2
  if [ "${#NODES[@]}" -eq 0 ]; then
    echo "[coordinator] no nodes configured; using built-in defaults" >&2
    NODES=("server2-labo=local:execute" "fedora=ssh:fedora:direct" "server1-k3s-worker=vm:vm:direct")
  fi

  for spec in "${NODES[@]}"; do
    if [ -z "${spec:-}" ]; then
      echo "[coordinator] skipped empty node spec" >&2
      continue
    fi
    echo "[coordinator] node spec: $spec" >&2
    start_agent_for_node "$spec" "$agent_mode"
  done

  if [ "$WATCH" = true ] || [ "$CMD" = "ecofloc" ]; then
    screen_init
  fi

  local last_render_ms="0" now_ms="0" interval_ms=""
  interval_ms="$(awk -v x="$WATCH_INTERVAL" 'BEGIN { printf "%d", x * 1000 }')"
  [ "$interval_ms" -le 0 ] 2>/dev/null && interval_ms="500"

  if [ "$CMD" = "get" ]; then
    while IFS= read -r line; do
      handle_event_line "$line"
    done < "$COORD_FIFO"
    render_table
    coord_cleanup 0
  fi

  while IFS= read -r line; do
    handle_event_line "$line"
    now_ms="$(date +%s%3N)"
    if [ "$last_render_ms" = "0" ] || [ $((now_ms - last_render_ms)) -ge "$interval_ms" ]; then
      coord_render_frame
      last_render_ms="$now_ms"
    fi
  done < "$COORD_FIFO"
}

# -----------------------------------------------------------------------------
# Argument parsing
# -----------------------------------------------------------------------------

if [ "${1:-}" = "__agent__" ]; then
  agent_main "$@"
fi

if [ $# -eq 0 ]; then
  print_help
  exit 1
fi

while [ $# -gt 0 ]; do
  case "$1" in
    --full) FULL=true; shift ;;
    --watch) WATCH=true; shift ;;
    --interval) WATCH_INTERVAL="$2"; shift 2 ;;
    --scan-interval) SCAN_INTERVAL="$2"; shift 2 ;;
    --ecofloc-interval) ECOFLOC_INTERVAL="$2"; shift 2 ;;
    --metrics) ECOFLOC_METRICS="$2"; shift 2 ;;
    --export) ECOFLOC_EXPORT_PATH="$2"; shift 2 ;;
    --node)
      if [ "$CUSTOM_NODES" = false ]; then
        NODES=()
        CUSTOM_NODES=true
      fi
      NODES+=("$2")
      shift 2
      ;;
    -h|--help) print_help; exit 0 ;;
    get|ecofloc) CMD="$1"; shift ;;
    *) echo "Unknown argument: $1" >&2; print_help; exit 1 ;;
  esac
done

case "$CMD" in
  get|ecofloc) coord_main ;;
  "") print_help; exit 1 ;;
  *) echo "Unknown command: $CMD" >&2; print_help; exit 1 ;;
esac