#!/usr/bin/env bash

# process.sh
# Monitor Python /app/... processes and attach EcoFloc using:
#   sudo /bin/execute ecofloc ...
#
# Main ideas:
# - Detect only real Python worker processes, not Argo wrappers.
# - Start EcoFloc collectors as soon as a PID appears.
# - Keep collectors alive with -t -1.
# - Stop collectors with SIGINT when the target PID disappears.
# - Only the main script writes state files.
# - No background worker writes to the table.
# - No kill -0 for target PID existence checks, because root-owned container PIDs
#   can make kill -0 fail for permission reasons.

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

COMMAND_WRAPPER=()

WATCH_INTERVAL="0.5"
SCAN_INTERVAL="0.1"

ECOFLOC_INTERVAL="1000"
ECOFLOC_METRICS="cpu,ram,sd,nic,gpu"
ECOFLOC_LOG_DIR="/tmp/erctl-ecofloc"
ECOFLOC_EXPORT_PATH=""

ROWS_FILE=""
SESSIONS_FILE=""
RESULTS_FILE=""
SUDO_KEEPALIVE_PID=""
CLEANUP_DONE=false

print_help() {
  cat <<'EOF'
Usage: process.sh [options] [command]

Commands:
  get                       Show Python PIDs running /app/... scripts.
  ecofloc                   Monitor detected Python PIDs with EcoFloc.

Options:
  --full                    Show full process commands.
  --watch                   Refresh continuously.
  --interval SECONDS        Table refresh interval. Default: 0.5.
  --scan-interval SECONDS   PID detection interval. Default: 0.1.

EcoFloc options:
  --ecofloc-interval MS     EcoFloc measurement interval in ms. Default: 1000.
  --metrics LIST            Comma-separated metrics. Default: cpu,ram,sd,nic,gpu.
                            Example: --metrics cpu,ram
  --export PATH             Pass -f PATH to EcoFloc.

Other:
  --help                    Show this help.

Examples:
  process.sh get
  process.sh --watch get
  process.sh ecofloc
  process.sh --watch ecofloc
  process.sh --watch ecofloc --metrics cpu,ram
  process.sh --watch ecofloc --metrics cpu,ram --scan-interval 0.05 --interval 0.25
EOF
}

run_wrapped() {
  "${COMMAND_WRAPPER[@]}" "$@"
}

hide_cursor() {
  tput civis 2>/dev/null || true
}

show_cursor() {
  tput cnorm 2>/dev/null || true
}

restore_terminal() {
  show_cursor
  stty sane 2>/dev/null || true
}

screen_init() {
  hide_cursor
  printf '\033[2J\033[H'
}

render_file_in_place() {
  local file="$1"

  printf '\033[H'
  cat "$file"
  printf '\033[J'
}

start_sudo_keepalive() {
  sudo -v

  (
    while true; do
      sudo -n true 2>/dev/null || exit 0
      sleep 60
    done
  ) &

  SUDO_KEEPALIVE_PID="$!"
}

stop_sudo_keepalive() {
  if [ -n "${SUDO_KEEPALIVE_PID:-}" ]; then
    kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
    wait "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
    SUDO_KEEPALIVE_PID=""
  fi
}

# Important:
# Do not use kill -0 here.
# Container processes are often root-owned, and kill -0 may return EPERM.
pid_exists() {
  local pid="$1"

  if [ -d "/proc/$pid" ]; then
    return 0
  fi

  ps -p "$pid" >/dev/null 2>&1
}

ensure_ecofloc_log_dir() {
  mkdir -p "$ECOFLOC_LOG_DIR"
}

stop_ecofloc_pid() {
  local eco_pid="$1"

  [ -n "${eco_pid:-}" ] || return 0

  # EcoFloc prints its summary on SIGINT.
  # We start it with setsid, so try the process group first.
  kill -INT "-${eco_pid}" 2>/dev/null || true
  kill -INT "$eco_pid" 2>/dev/null || true

  sleep 1

  if ps -p "$eco_pid" >/dev/null 2>&1; then
    kill -TERM "-${eco_pid}" 2>/dev/null || true
    kill -TERM "$eco_pid" 2>/dev/null || true
  fi

  sleep 0.5

  if ps -p "$eco_pid" >/dev/null 2>&1; then
    kill -KILL "-${eco_pid}" 2>/dev/null || true
    kill -KILL "$eco_pid" 2>/dev/null || true
  fi
}

stop_all_ecofloc_sessions() {
  local pid=""
  local metric=""
  local eco_pid=""
  local log_file=""
  local start_ts=""

  [ -n "${SESSIONS_FILE:-}" ] || return 0
  [ -f "$SESSIONS_FILE" ] || return 0

  while IFS=$'\t' read -r pid metric eco_pid log_file start_ts; do
    [ -n "${eco_pid:-}" ] || continue
    stop_ecofloc_pid "$eco_pid"
  done < "$SESSIONS_FILE"
}

cleanup() {
  local exit_code="${1:-0}"

  if [ "$CLEANUP_DONE" = true ]; then
    exit "$exit_code"
  fi

  CLEANUP_DONE=true

  stop_all_ecofloc_sessions >/dev/null 2>&1 || true
  stop_sudo_keepalive >/dev/null 2>&1 || true
  restore_terminal

  [ -n "${ROWS_FILE:-}" ] && rm -f "$ROWS_FILE" 2>/dev/null || true
  [ -n "${SESSIONS_FILE:-}" ] && rm -f "$SESSIONS_FILE" 2>/dev/null || true
  [ -n "${RESULTS_FILE:-}" ] && rm -f "$RESULTS_FILE" 2>/dev/null || true

  exit "$exit_code"
}

handle_stop_signal() {
  cleanup 130
}

get_raw_processes() {
  run_wrapped ps axww -o pid=,args=
}

filter_processes() {
  python3 -c '
import os
import sys

SELF_PATTERNS = [
    "sudo /bin/execute ecofloc",
    "/bin/execute ecofloc",
    "/opt/ecofloc/ecofloc",
    " ecofloc ",
    "process.sh",
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

    # Important:
    # Only accept actual Python worker processes where argv[0] is Python
    # and argv[1] is an /app/... script.
    # This avoids sh -c wrappers and Argo executor wrappers.
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

get_matching_processes() {
  get_raw_processes | filter_processes
}

print_process_table() {
  python3 -c '
import os
import re
import sys

PID_W = 8
SCRIPT_W = 40
FUNC_W = 32

def fit(value, width):
    value = str(value)
    if len(value) <= width:
        return value
    return value[:max(width - 1, 0)] + "…"

def base(path):
    return os.path.basename(path.rstrip())

def tokens(cmd):
    return cmd.split()

def clean(value):
    return value.strip().strip("\"").strip(chr(39))

def extract_script(cmd):
    ts = tokens(cmd)
    if len(ts) >= 2 and ts[1].startswith("/app/"):
        return base(ts[1])
    return "-"

def extract_function(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if tok == "--function" and i + 1 < len(ts):
            return clean(ts[i + 1])

        if tok.startswith("--function="):
            return clean(tok.split("=", 1)[1])

    m = re.search(r"--function(?:=|\s+)([^ ]+)", cmd)
    if m:
        return clean(m.group(1))

    return "-"

print("{:<{}} | {:<{}} | {:<{}}".format("PID", PID_W, "SCRIPT", SCRIPT_W, "FUNCTION", FUNC_W))
print("{}-+-{}-+-{}".format("-" * PID_W, "-" * SCRIPT_W, "-" * FUNC_W))

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line or "\t" not in line:
        continue

    pid, cmd = line.split("\t", 1)

    print("{:<{}} | {:<{}} | {:<{}}".format(
        fit(pid, PID_W), PID_W,
        fit(extract_script(cmd), SCRIPT_W), SCRIPT_W,
        fit(extract_function(cmd), FUNC_W), FUNC_W
    ))
'
}

run_get_once() {
  if [ "${QUIET_HEADER:-false}" != true ]; then
    echo "Getting PID of Python processes running in the Argo workflow..."
  fi

  if [ "$FULL" = true ]; then
    get_matching_processes
  else
    get_matching_processes | print_process_table
  fi
}

append_row_if_missing() {
  local pid="$1"
  local cmd="$2"

  python3 - "$ROWS_FILE" "$pid" "$cmd" <<'PY'
import sys

rows_file = sys.argv[1]
pid = sys.argv[2]
cmd = sys.argv[3]

try:
    with open(rows_file, "r", encoding="utf-8") as f:
        rows = f.read().splitlines()
except FileNotFoundError:
    rows = []

for row in rows:
    parts = row.split("\t", 1)
    if parts and parts[0] == pid:
        sys.exit(0)

rows.append(f"{pid}\t{cmd}")

with open(rows_file, "w", encoding="utf-8") as f:
    f.write("\n".join(rows) + "\n")
PY
}

session_exists() {
  local pid="$1"
  local metric="$2"

  grep -q "^${pid}"$'\t'"${metric}"$'\t' "$SESSIONS_FILE" 2>/dev/null
}

result_exists() {
  local pid="$1"
  local metric="$2"

  grep -q "^${pid}"$'\t'"${metric}"$'\t' "$RESULTS_FILE" 2>/dev/null
}

write_result() {
  local pid="$1"
  local metric="$2"
  local value="$3"
  local tmp=""

  tmp="$(mktemp)"

  if [ -f "$RESULTS_FILE" ]; then
    grep -v "^${pid}"$'\t'"${metric}"$'\t' "$RESULTS_FILE" > "$tmp" || true
  fi

  printf "%s\t%s\t%s\n" "$pid" "$metric" "$value" >> "$tmp"
  mv "$tmp" "$RESULTS_FILE"
}

start_ecofloc_session() {
  local pid="$1"
  local metric="$2"
  local log_file="$3"
  local export_args=()

  ensure_ecofloc_log_dir

  if [ -n "$ECOFLOC_EXPORT_PATH" ]; then
    export_args=(-f "$ECOFLOC_EXPORT_PATH")
  fi

  if command -v setsid >/dev/null 2>&1; then
    setsid sudo /bin/execute ecofloc "--${metric}" \
      -p "$pid" \
      -i "$ECOFLOC_INTERVAL" \
      -t -1 \
      "${export_args[@]}" > "$log_file" 2>&1 &
  else
    sudo /bin/execute ecofloc "--${metric}" \
      -p "$pid" \
      -i "$ECOFLOC_INTERVAL" \
      -t -1 \
      "${export_args[@]}" > "$log_file" 2>&1 &
  fi

  echo "$!"
}

parse_ecofloc_log() {
  local log_file="$1"

  if [ ! -f "$log_file" ]; then
    echo "NO_DATA"
    return 0
  fi

  local avg=""
  local total=""

  avg="$(
    awk -F ':' '
      /Average Power/ {
        gsub(/^[ \t]+|[ \t]+$/, "", $2)
        last = $2
      }
      END {
        if (last != "") print last
      }
    ' "$log_file" | awk '{ print $1 }'
  )"

  total="$(
    awk -F ':' '
      /Total.*Energy/ {
        gsub(/^[ \t]+|[ \t]+$/, "", $2)
        last = $2
      }
      END {
        if (last != "") print last
      }
    ' "$log_file" | awk '{ print $1 }'
  )"

  if [ -z "$avg" ] && [ -z "$total" ]; then
    if grep -q "Usage:" "$log_file" 2>/dev/null; then
      echo "CMD_ERR"
    else
      echo "NO_DATA"
    fi
    return 0
  fi

  if [ -z "$avg" ]; then
    avg="?"
  fi

  if [ -z "$total" ]; then
    total="?"
  fi

  echo "${avg}W/${total}J"
}

create_sessions_for_matches() {
  local matches="$1"
  local pid=""
  local cmd=""
  local metric=""
  local log_file=""
  local eco_pid=""
  local start_ts=""

  while IFS=$'\t' read -r pid cmd; do
    [ -n "${pid:-}" ] || continue
    [ -n "${cmd:-}" ] || continue

    append_row_if_missing "$pid" "$cmd"

    IFS=',' read -r -a metric_list <<< "$ECOFLOC_METRICS"

    for metric in "${metric_list[@]}"; do
      metric="$(printf "%s" "$metric" | tr '[:upper:]' '[:lower:]' | xargs)"

      case "$metric" in
        cpu|ram|sd|nic|gpu)
          if result_exists "$pid" "$metric"; then
            continue
          fi

          if session_exists "$pid" "$metric"; then
            continue
          fi

          if ! pid_exists "$pid"; then
            write_result "$pid" "$metric" "TOO_SHORT"
            continue
          fi

          start_ts="$(date +%s)"
          log_file="${ECOFLOC_LOG_DIR}/ecofloc_${pid}_${metric}_${start_ts}.log"
          eco_pid="$(start_ecofloc_session "$pid" "$metric" "$log_file")"

          printf "%s\t%s\t%s\t%s\t%s\n" "$pid" "$metric" "$eco_pid" "$log_file" "$start_ts" >> "$SESSIONS_FILE"
          ;;
        *)
          ;;
      esac
    done
  done <<< "$matches"
}

finalize_finished_sessions() {
  local tmp=""
  local pid=""
  local metric=""
  local eco_pid=""
  local log_file=""
  local start_ts=""
  local now=""
  local elapsed=""
  local value=""

  [ -f "$SESSIONS_FILE" ] || return 0

  tmp="$(mktemp)"
  now="$(date +%s)"

  while IFS=$'\t' read -r pid metric eco_pid log_file start_ts; do
    [ -n "${pid:-}" ] || continue
    [ -n "${metric:-}" ] || continue
    [ -n "${eco_pid:-}" ] || continue
    [ -n "${log_file:-}" ] || continue
    [ -n "${start_ts:-}" ] || start_ts="$now"

    if pid_exists "$pid"; then
      printf "%s\t%s\t%s\t%s\t%s\n" "$pid" "$metric" "$eco_pid" "$log_file" "$start_ts" >> "$tmp"
      continue
    fi

    elapsed="$((now - start_ts))"

    stop_ecofloc_pid "$eco_pid"

    value="$(parse_ecofloc_log "$log_file" | tail -n 1 | tr -d '\r')"

    if [ -z "$value" ] || [ "$value" = "NO_DATA" ]; then
      if [ "$elapsed" -lt 1 ]; then
        value="TOO_SHORT"
      else
        value="NO_DATA"
      fi
    fi

    write_result "$pid" "$metric" "$value"
  done < "$SESSIONS_FILE"

  mv "$tmp" "$SESSIONS_FILE"
}

render_ecofloc_table() {
  python3 - "$ROWS_FILE" "$SESSIONS_FILE" "$RESULTS_FILE" "$ECOFLOC_METRICS" <<'PY'
import os
import re
import sys
import time

rows_file = sys.argv[1]
sessions_file = sys.argv[2]
results_file = sys.argv[3]
enabled_metrics = {m.strip().lower() for m in sys.argv[4].split(",") if m.strip()}

PID_W = 8
SCRIPT_W = 30
FUNC_W = 28
METRIC_W = 15

def fit(value, width):
    value = str(value)
    if len(value) <= width:
        return value
    return value[:max(width - 1, 0)] + "…"

def base(path):
    return os.path.basename(path.rstrip())

def tokens(cmd):
    return cmd.split()

def clean(value):
    return value.strip().strip("\"").strip("'")

def extract_script(cmd):
    ts = tokens(cmd)
    if len(ts) >= 2 and ts[1].startswith("/app/"):
        return base(ts[1])
    return "-"

def extract_function(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if tok == "--function" and i + 1 < len(ts):
            return clean(ts[i + 1])

        if tok.startswith("--function="):
            return clean(tok.split("=", 1)[1])

    m = re.search(r"--function(?:=|\s+)([^ ]+)", cmd)
    if m:
        return clean(m.group(1))

    return "-"

results = {}
sessions = {}
now = int(time.time())

try:
    with open(results_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            parts = line.split("\t", 2)
            if len(parts) == 3:
                pid, metric, value = parts
                results[(pid, metric)] = value
except FileNotFoundError:
    pass

try:
    with open(sessions_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            parts = line.split("\t")
            if len(parts) >= 5:
                pid, metric, eco_pid, log_file, start_ts = parts[:5]
                try:
                    elapsed = max(0, now - int(start_ts))
                except ValueError:
                    elapsed = 0
                sessions[(pid, metric)] = f"RUN {elapsed}s"
except FileNotFoundError:
    pass

def value_for(pid, metric):
    if metric not in enabled_metrics:
        return "-"

    if (pid, metric) in results:
        return results[(pid, metric)]

    if (pid, metric) in sessions:
        return sessions[(pid, metric)]

    return "WAIT"

headers = ["PID", "SCRIPT", "FUNCTION", "CPU", "RAM", "SD", "NIC", "GPU"]
widths = [PID_W, SCRIPT_W, FUNC_W, METRIC_W, METRIC_W, METRIC_W, METRIC_W, METRIC_W]

print(" | ".join("{:<{}}".format(h, w) for h, w in zip(headers, widths)))
print("-+-".join("-" * w for w in widths))

try:
    with open(rows_file, "r", encoding="utf-8") as f:
        rows = f.read().splitlines()
except FileNotFoundError:
    rows = []

for row in rows:
    if not row.strip():
        continue

    parts = row.split("\t", 1)
    if len(parts) != 2:
        continue

    pid, cmd = parts

    values = [
        pid,
        extract_script(cmd),
        extract_function(cmd),
        value_for(pid, "cpu"),
        value_for(pid, "ram"),
        value_for(pid, "sd"),
        value_for(pid, "nic"),
        value_for(pid, "gpu"),
    ]

    print(" | ".join("{:<{}}".format(fit(v, w), w) for v, w in zip(values, widths)))
PY
}

render_ecofloc_frame() {
  local status="$1"
  local frame_file=""

  frame_file="$(mktemp)"

  {
    echo "EcoFloc PID monitoring. Press Ctrl+C to stop."
    echo "Status: ${status}"
    echo

    if [ -s "$ROWS_FILE" ]; then
      render_ecofloc_table
    else
      echo "No matching Argo workflow process found."
    fi
  } > "$frame_file"

  render_file_in_place "$frame_file"
  rm -f "$frame_file"
}

run_ecofloc_watch() {
  local matches=""
  local status=""
  local active=""
  local last_render_ns="0"
  local now_ns="0"

  start_sudo_keepalive
  ensure_ecofloc_log_dir

  ROWS_FILE="$(mktemp)"
  SESSIONS_FILE="$(mktemp)"
  RESULTS_FILE="$(mktemp)"

  trap 'cleanup 0' EXIT
  trap handle_stop_signal INT TERM HUP QUIT TSTP

  screen_init

  while true; do
    matches="$(get_matching_processes || true)"

    if [ -n "$matches" ]; then
      create_sessions_for_matches "$matches"
    fi

    finalize_finished_sessions

    active="$(wc -l < "$SESSIONS_FILE" | xargs)"

    if [ "$active" != "0" ]; then
      status="monitoring; active EcoFloc sessions: ${active}"
    elif [ -s "$ROWS_FILE" ]; then
      status="waiting for new processes"
    else
      status="waiting for matching processes"
    fi

    now_ns="$(date +%s%N)"

    if [ "$last_render_ns" = "0" ]; then
      render_ecofloc_frame "$status"
      last_render_ns="$now_ns"
    else
      if python3 - "$last_render_ns" "$now_ns" "$WATCH_INTERVAL" <<'PY'
import sys
last_ns = int(sys.argv[1])
now_ns = int(sys.argv[2])
interval = float(sys.argv[3])
elapsed = (now_ns - last_ns) / 1_000_000_000
sys.exit(0 if elapsed >= interval else 1)
PY
      then
        render_ecofloc_frame "$status"
        last_render_ns="$now_ns"
      fi
    fi

    sleep "$SCAN_INTERVAL"
  done
}

run_ecofloc_once() {
  local matches=""
  local active=""

  start_sudo_keepalive
  ensure_ecofloc_log_dir

  ROWS_FILE="$(mktemp)"
  SESSIONS_FILE="$(mktemp)"
  RESULTS_FILE="$(mktemp)"

  trap 'cleanup 0' EXIT
  trap handle_stop_signal INT TERM HUP QUIT TSTP

  screen_init

  matches="$(get_matching_processes || true)"

  if [ -z "$matches" ]; then
    echo "No matching Argo workflow process found."
    cleanup 0
  fi

  create_sessions_for_matches "$matches"

  while true; do
    finalize_finished_sessions

    active="$(wc -l < "$SESSIONS_FILE" | xargs)"
    render_ecofloc_frame "monitoring; active EcoFloc sessions: ${active}"

    if [ "$active" = "0" ]; then
      break
    fi

    sleep "$WATCH_INTERVAL"
  done

  render_ecofloc_frame "done"
  cleanup 0
}

render_process_watch_frame() {
  local frame_file=""

  frame_file="$(mktemp)"

  {
    echo "Watching Argo workflow Python processes. Press Ctrl+C to stop."
    echo
    QUIET_HEADER=true run_get_once || true
  } > "$frame_file"

  render_file_in_place "$frame_file"
  rm -f "$frame_file"
}

run_get_watch() {
  trap 'restore_terminal; exit 0' EXIT INT TERM HUP QUIT TSTP

  screen_init

  while true; do
    render_process_watch_frame
    sleep "$WATCH_INTERVAL"
  done
}

if [ $# -eq 0 ]; then
  print_help
  exit 1
fi

FULL=false
WATCH=false
CMD=""

while [ $# -gt 0 ]; do
  case "$1" in
    --full)
      FULL=true
      shift
      ;;

    --watch)
      WATCH=true
      shift
      ;;

    --interval)
      if [ $# -lt 2 ]; then
        echo "Missing value for --interval"
        exit 1
      fi
      WATCH_INTERVAL="$2"
      shift 2
      ;;

    --scan-interval)
      if [ $# -lt 2 ]; then
        echo "Missing value for --scan-interval"
        exit 1
      fi
      SCAN_INTERVAL="$2"
      shift 2
      ;;

    --ecofloc-interval)
      if [ $# -lt 2 ]; then
        echo "Missing value for --ecofloc-interval"
        exit 1
      fi
      ECOFLOC_INTERVAL="$2"
      shift 2
      ;;

    --metrics)
      if [ $# -lt 2 ]; then
        echo "Missing value for --metrics"
        exit 1
      fi
      ECOFLOC_METRICS="$2"
      shift 2
      ;;

    --export)
      if [ $# -lt 2 ]; then
        echo "Missing value for --export"
        exit 1
      fi
      ECOFLOC_EXPORT_PATH="$2"
      shift 2
      ;;

    -h|--help)
      CMD="--help"
      shift
      ;;

    *)
      if [ -z "$CMD" ]; then
        CMD="$1"
      fi
      shift
      ;;
  esac
done

case "$CMD" in
  get)
    if [ "$WATCH" = true ]; then
      run_get_watch
    else
      run_get_once
    fi
    ;;

  ecofloc)
    if [ "$WATCH" = true ]; then
      run_ecofloc_watch
    else
      run_ecofloc_once
    fi
    ;;

  --help)
    print_help
    exit 0
    ;;

  "")
    print_help
    exit 1
    ;;

  *)
    echo "Unknown command: $CMD"
    print_help
    exit 1
    ;;
esac