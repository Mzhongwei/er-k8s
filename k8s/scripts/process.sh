#!/usr/bin/env bash

# Interact with Python processes running in an Argo workflow.

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

# Server mode: commands run directly on the current machine.
COMMAND_WRAPPER=()

WATCH_INTERVAL="0.5"
SCAN_INTERVAL="0.1"

ECOFLOC_INTERVAL="1000"
ECOFLOC_WINDOW="1"
ECOFLOC_METRICS="cpu,ram,sd,nic,gpu"
ECOFLOC_EXPORT_PATH=""
ECOFLOC_USE_SUDO_EXECUTE=true
ECOFLOC_LOG_DIR="/tmp/erctl-ecofloc"

SUDO_KEEPALIVE_PID=""
GLOBAL_ROWS_FILE=""
GLOBAL_WORKERS_FILE=""
GLOBAL_PIDS_FILE=""
CLEANUP_DONE=false

print_help() {
  cat <<'EOF'
Usage: process.sh [options] [command]

Commands:
  get                       Get Python PIDs running /app/... scripts.
  ecofloc                   Monitor detected Python PIDs with EcoFloc.

Options:
  --full                    Show full matching process commands.
  --watch                   Refresh continuously.
  --interval SECONDS        Display refresh interval. Default: 0.5.
  --scan-interval SECONDS   PID detection interval. Default: 0.1.

EcoFloc options:
  --ecofloc-interval MS     EcoFloc measurement interval in milliseconds. Default: 1000.
  --ecofloc-window SECONDS  EcoFloc window duration per sample. Default: 1.
  --metrics LIST            Comma-separated metrics. Default: cpu,ram,sd,nic,gpu.
                            Example: --metrics cpu,ram
  --export PATH             Pass -f PATH to EcoFloc.
  --no-sudo-execute         Run ecofloc directly instead of sudo /bin/execute ecofloc.

Other:
  --help                    Show this help.

Examples:
  process.sh get
  process.sh --watch get
  process.sh ecofloc
  process.sh --watch ecofloc
  process.sh --watch ecofloc --metrics cpu,ram
  process.sh --watch ecofloc --scan-interval 0.05 --interval 0.25
EOF
}

run_wrapped() {
  "${COMMAND_WRAPPER[@]}" "$@"
}

clear_screen() {
  printf '\033[2J\033[H'
}

move_home() {
  printf '\033[H'
}

clear_rest() {
  printf '\033[J'
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

start_sudo_keepalive() {
  if [ "$ECOFLOC_USE_SUDO_EXECUTE" != true ]; then
    return 0
  fi

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

kill_worker_processes() {
  local workers_file="$1"
  local worker_pid=""

  if [ ! -f "$workers_file" ]; then
    return 0
  fi

  while IFS= read -r worker_pid; do
    [ -n "$worker_pid" ] || continue
    kill -TERM "$worker_pid" 2>/dev/null || true
  done < "$workers_file"

  sleep 0.2

  while IFS= read -r worker_pid; do
    [ -n "$worker_pid" ] || continue
    kill -KILL "$worker_pid" 2>/dev/null || true
  done < "$workers_file"
}

global_cleanup() {
  local exit_code="${1:-0}"

  if [ "$CLEANUP_DONE" = true ]; then
    exit "$exit_code"
  fi

  CLEANUP_DONE=true

  if [ -n "${GLOBAL_WORKERS_FILE:-}" ] && [ -f "$GLOBAL_WORKERS_FILE" ]; then
    kill_worker_processes "$GLOBAL_WORKERS_FILE" >/dev/null 2>&1 || true
  fi

  stop_sudo_keepalive >/dev/null 2>&1 || true
  restore_terminal

  [ -n "${GLOBAL_ROWS_FILE:-}" ] && rm -f "$GLOBAL_ROWS_FILE" 2>/dev/null || true
  [ -n "${GLOBAL_WORKERS_FILE:-}" ] && rm -f "$GLOBAL_WORKERS_FILE" 2>/dev/null || true
  [ -n "${GLOBAL_PIDS_FILE:-}" ] && rm -f "$GLOBAL_PIDS_FILE" 2>/dev/null || true

  exit "$exit_code"
}

handle_stop_signal() {
  global_cleanup 130
}

get_raw_processes() {
  run_wrapped ps axww -o pid=,args=
}

filter_processes() {
  python3 -c '
import os
import re
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
    "python -u -m sidecar",
    "/usr/bin/python3 -sP /usr/bin/firewalld",
    "/usr/bin/python3 -Es /usr/sbin/tuned",
    "/usr/bin/python3 -Es /usr/sbin/tuned-ppd",
]

def parse_line(line):
    line = line.rstrip(\"\\n\")

    if not line.strip():
        return None, \"\"

    parts = line.strip().split(None, 1)

    if len(parts) == 1:
        return parts[0], \"\"

    return parts[0], parts[1]

def base(path):
    return os.path.basename(path.rstrip())

def tokens(cmd):
    return cmd.split()

def is_python_token(tok):
    b = base(tok)

    return (
        b == \"python\"
        or b == \"python3\"
        or b.startswith(\"python3.\")
        or b.endswith(\"python\")
        or b.startswith(\"python\")
        or tok.endswith(\"/bin/python\")
        or tok.endswith(\"/bin/python3\")
        or \"/python\" in tok
    )

def script_after_python(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if is_python_token(tok) and i + 1 < len(ts):
            candidate = ts[i + 1]

            if candidate.startswith(\"/app/\"):
                return candidate

    return \"\"

def should_ignore(cmd):
    if any(p in cmd for p in SELF_PATTERNS):
        return True

    if any(p in cmd for p in IGNORED_PATTERNS):
        return True

    return False

def is_target(cmd):
    if should_ignore(cmd):
        return False

    script = script_after_python(cmd)

    if script:
        return True

    return False

seen = set()

for line in sys.stdin:
    pid, cmd = parse_line(line)

    if not pid or not cmd:
        continue

    if pid in seen:
        continue

    if not is_target(cmd):
        continue

    seen.add(pid)
    print(f\"{pid}\\t{cmd}\")
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

    return value[:max(width - 1, 0)] + \"…\"

def base(path):
    return os.path.basename(path.rstrip())

def tokens(cmd):
    return cmd.split()

def clean(value):
    return value.strip().strip(\"\\\"\").strip(chr(39))

def is_python_token(tok):
    b = base(tok)

    return (
        b == \"python\"
        or b == \"python3\"
        or b.startswith(\"python3.\")
        or b.endswith(\"python\")
        or b.startswith(\"python\")
        or tok.endswith(\"/bin/python\")
        or tok.endswith(\"/bin/python3\")
        or \"/python\" in tok
    )

def extract_script(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if is_python_token(tok) and i + 1 < len(ts):
            candidate = ts[i + 1]

            if candidate.startswith(\"/app/\"):
                return base(candidate)

    m = re.search(r\"(?<!\\\\S)/app/[^ ]+\\\\.py(?!\\\\S)\", cmd)

    if m:
        return base(m.group(0))

    return \"-\"

def extract_function(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if tok == \"--function\" and i + 1 < len(ts):
            return clean(ts[i + 1])

        if tok.startswith(\"--function=\"):
            return clean(tok.split(\"=\", 1)[1])

    m = re.search(r\"--function(?:=|\\\\s+)([^ ]+)\", cmd)

    if m:
        return clean(m.group(1))

    return \"-\"

print(\"{:<{}} | {:<{}} | {:<{}}\".format(\"PID\", PID_W, \"SCRIPT\", SCRIPT_W, \"FUNCTION\", FUNC_W))
print(\"{}-+-{}-+-{}\".format(\"-\" * PID_W, \"-\" * SCRIPT_W, \"-\" * FUNC_W))

for line in sys.stdin:
    line = line.rstrip(\"\\n\")

    if not line or \"\\t\" not in line:
        continue

    pid, cmd = line.split(\"\\t\", 1)

    print(\"{:<{}} | {:<{}} | {:<{}}\".format(
        fit(pid, PID_W), PID_W,
        fit(extract_script(cmd), SCRIPT_W), SCRIPT_W,
        fit(extract_function(cmd), FUNC_W), FUNC_W
    ))
'
}

print_ecofloc_table() {
  python3 -c '
import os
import re
import sys

PID_W = 8
SCRIPT_W = 30
FUNC_W = 28
METRIC_W = 15

def fit(value, width):
    value = str(value)

    if len(value) <= width:
        return value

    return value[:max(width - 1, 0)] + \"…\"

def base(path):
    return os.path.basename(path.rstrip())

def tokens(cmd):
    return cmd.split()

def clean(value):
    return value.strip().strip(\"\\\"\").strip(chr(39))

def is_python_token(tok):
    b = base(tok)

    return (
        b == \"python\"
        or b == \"python3\"
        or b.startswith(\"python3.\")
        or b.endswith(\"python\")
        or b.startswith(\"python\")
        or tok.endswith(\"/bin/python\")
        or tok.endswith(\"/bin/python3\")
        or \"/python\" in tok
    )

def extract_script(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if is_python_token(tok) and i + 1 < len(ts):
            candidate = ts[i + 1]

            if candidate.startswith(\"/app/\"):
                return base(candidate)

    m = re.search(r\"(?<!\\\\S)/app/[^ ]+\\\\.py(?!\\\\S)\", cmd)

    if m:
        return base(m.group(0))

    return \"-\"

def extract_function(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if tok == \"--function\" and i + 1 < len(ts):
            return clean(ts[i + 1])

        if tok.startswith(\"--function=\"):
            return clean(tok.split(\"=\", 1)[1])

    m = re.search(r\"--function(?:=|\\\\s+)([^ ]+)\", cmd)

    if m:
        return clean(m.group(1))

    return \"-\"

headers = [\"PID\", \"SCRIPT\", \"FUNCTION\", \"CPU\", \"RAM\", \"SD\", \"NIC\", \"GPU\"]
widths = [PID_W, SCRIPT_W, FUNC_W, METRIC_W, METRIC_W, METRIC_W, METRIC_W, METRIC_W]

print(\" | \".join(\"{:<{}}\".format(h, w) for h, w in zip(headers, widths)))
print(\"-+-\".join(\"-\" * w for w in widths))

for line in sys.stdin:
    line = line.rstrip(\"\\n\")

    if not line:
        continue

    parts = line.split(\"\\t\")

    while len(parts) < 7:
        parts.append(\"-\")

    pid = parts[0]
    cmd = parts[1]
    cpu = parts[2]
    ram = parts[3]
    sd = parts[4]
    nic = parts[5]
    gpu = parts[6]

    values = [
        pid,
        extract_script(cmd),
        extract_function(cmd),
        cpu,
        ram,
        sd,
        nic,
        gpu,
    ]

    print(\" | \".join(\"{:<{}}\".format(fit(v, w), w) for v, w in zip(values, widths)))
'
}

run_once() {
  if [ "${QUIET_HEADER:-false}" != true ]; then
    echo "Getting PID of Python processes running in the Argo workflow..."
  fi

  if [ "$FULL" = true ]; then
    get_matching_processes
  else
    get_matching_processes | print_process_table
  fi
}

metric_enabled() {
  local target="$1"
  local metric=""

  IFS=',' read -r -a metric_list <<< "$ECOFLOC_METRICS"

  for metric in "${metric_list[@]}"; do
    metric="$(printf "%s" "$metric" | tr "[:upper:]" "[:lower:]" | xargs)"

    if [ "$metric" = "$target" ]; then
      return 0
    fi
  done

  return 1
}

pid_alive() {
  local pid="$1"
  kill -0 "$pid" 2>/dev/null
}

ensure_ecofloc_log_dir() {
  mkdir -p "$ECOFLOC_LOG_DIR"
}

ecofloc_command_prefix() {
  if [ "$ECOFLOC_USE_SUDO_EXECUTE" = true ]; then
    printf '%s\n' "sudo /bin/execute ecofloc"
  else
    printf '%s\n' "ecofloc"
  fi
}

run_ecofloc_window() {
  local pid="$1"
  local metric="$2"
  local log_file="$3"
  local export_arg=()

  ensure_ecofloc_log_dir

  if [ -n "$ECOFLOC_EXPORT_PATH" ]; then
    export_arg=(-f "$ECOFLOC_EXPORT_PATH")
  fi

  if [ "$ECOFLOC_USE_SUDO_EXECUTE" = true ]; then
    timeout "$((ECOFLOC_WINDOW + 3))" sudo /bin/execute ecofloc "--${metric}" \
      -p "$pid" \
      -i "$ECOFLOC_INTERVAL" \
      -t "$ECOFLOC_WINDOW" \
      "${export_arg[@]}" > "$log_file" 2>&1 || true
  else
    timeout "$((ECOFLOC_WINDOW + 3))" ecofloc "--${metric}" \
      -p "$pid" \
      -i "$ECOFLOC_INTERVAL" \
      -t "$ECOFLOC_WINDOW" \
      "${export_arg[@]}" > "$log_file" 2>&1 || true
  fi
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

safe_update_cell() {
  local file="$1"
  local target_pid="$2"
  local metric="$3"
  local value="$4"

  python3 - "$file" "$target_pid" "$metric" "$value" <<'PY'
import re
import sys

path = sys.argv[1]
target_pid = sys.argv[2]
metric = sys.argv[3].lower()
new_value = sys.argv[4]

index_by_metric = {
    "cpu": 2,
    "ram": 3,
    "sd": 4,
    "nic": 5,
    "gpu": 6,
}

idx = index_by_metric.get(metric)

if idx is None:
    sys.exit(0)

bad_values = {"", "N/A", "ERR", "CMD_ERR", "NO_DATA"}
final_re = re.compile(r"^[0-9.?]+W/[0-9.?]+J$")

try:
    with open(path, "r", encoding="utf-8") as f:
        rows = f.read().splitlines()
except FileNotFoundError:
    rows = []

new_rows = []

for row in rows:
    parts = row.split("\t")

    while len(parts) < 7:
        parts.append("-")

    if parts[0] == target_pid:
        old_value = parts[idx]
        old_is_final = bool(final_re.match(old_value))
        new_is_bad = new_value in bad_values

        if old_is_final and new_is_bad:
            pass
        else:
            parts[idx] = new_value

    new_rows.append("\t".join(parts))

with open(path, "w", encoding="utf-8") as f:
    if new_rows:
        f.write("\n".join(new_rows) + "\n")
PY
}

append_row_if_missing() {
  local file="$1"
  local pid="$2"
  local cmd="$3"

  python3 - "$file" "$pid" "$cmd" "$ECOFLOC_METRICS" <<'PY'
import sys

path = sys.argv[1]
pid = sys.argv[2]
cmd = sys.argv[3]
metrics = {m.strip().lower() for m in sys.argv[4].split(",") if m.strip()}

try:
    with open(path, "r", encoding="utf-8") as f:
        rows = f.read().splitlines()
except FileNotFoundError:
    rows = []

for row in rows:
    parts = row.split("\t")
    if parts and parts[0] == pid:
        sys.exit(0)

cpu = "WAIT" if "cpu" in metrics else "-"
ram = "WAIT" if "ram" in metrics else "-"
sd = "WAIT" if "sd" in metrics else "-"
nic = "WAIT" if "nic" in metrics else "-"
gpu = "WAIT" if "gpu" in metrics else "-"

rows.append("\t".join([pid, cmd, cpu, ram, sd, nic, gpu]))

with open(path, "w", encoding="utf-8") as f:
    f.write("\n".join(rows) + "\n")
PY
}

worker_key_exists() {
  local pids_file="$1"
  local pid="$2"
  local metric="$3"

  grep -q "^${pid}"$'\t'"${metric}"$ "$pids_file" 2>/dev/null
}

mark_worker_key() {
  local pids_file="$1"
  local pid="$2"
  local metric="$3"

  printf "%s\t%s\n" "$pid" "$metric" >> "$pids_file"
}

remove_worker_key() {
  local pids_file="$1"
  local pid="$2"
  local metric="$3"
  local tmp=""

  tmp="$(mktemp)"
  grep -v "^${pid}"$'\t'"${metric}"$ "$pids_file" 2>/dev/null > "$tmp" || true
  mv "$tmp" "$pids_file"
}

monitor_pid_metric_once() {
  local rows_file="$1"
  local pids_file="$2"
  local pid="$3"
  local metric="$4"
  local log_file=""
  local result=""

  log_file="${ECOFLOC_LOG_DIR}/ecofloc_${pid}_${metric}_$(date +%s%N).log"

  safe_update_cell "$rows_file" "$pid" "$metric" "RUN"

  if pid_alive "$pid"; then
    run_ecofloc_window "$pid" "$metric" "$log_file"
    result="$(parse_ecofloc_log "$log_file" | tail -n 1 | tr -d '\r')"

    if [ -z "$result" ]; then
      result="NO_DATA"
    fi

    safe_update_cell "$rows_file" "$pid" "$metric" "$result"
  else
    safe_update_cell "$rows_file" "$pid" "$metric" "ENDED"
  fi

  remove_worker_key "$pids_file" "$pid" "$metric"
}

start_metric_worker_if_needed() {
  local rows_file="$1"
  local workers_file="$2"
  local pids_file="$3"
  local pid="$4"
  local metric="$5"

  if worker_key_exists "$pids_file" "$pid" "$metric"; then
    return 0
  fi

  mark_worker_key "$pids_file" "$pid" "$metric"

  (
    monitor_pid_metric_once "$rows_file" "$pids_file" "$pid" "$metric"
  ) &

  printf "%s\n" "$!" >> "$workers_file"
}

create_workers_for_matches() {
  local matches="$1"
  local rows_file="$2"
  local workers_file="$3"
  local pids_file="$4"
  local pid=""
  local cmd=""
  local metric=""

  while IFS=$'\t' read -r pid cmd; do
    [ -n "${pid:-}" ] || continue
    [ -n "${cmd:-}" ] || continue

    append_row_if_missing "$rows_file" "$pid" "$cmd"

    IFS=',' read -r -a metric_list <<< "$ECOFLOC_METRICS"

    for metric in "${metric_list[@]}"; do
      metric="$(printf "%s" "$metric" | tr "[:upper:]" "[:lower:]" | xargs)"

      case "$metric" in
        cpu|ram|sd|nic|gpu)
          start_metric_worker_if_needed "$rows_file" "$workers_file" "$pids_file" "$pid" "$metric"
          ;;
        *)
          ;;
      esac
    done
  done <<< "$matches"
}

render_ecofloc_frame() {
  local rows_file="$1"
  local status="$2"
  local frame_file=""

  frame_file="$(mktemp)"

  {
    echo "EcoFloc PID monitoring. Press Ctrl+C to stop."
    echo "Status: ${status}"
    echo

    if [ -s "$rows_file" ]; then
      cat "$rows_file" | print_ecofloc_table
    else
      echo "No matching Argo workflow process found."
    fi
  } > "$frame_file"

  move_home
  cat "$frame_file"
  clear_rest

  rm -f "$frame_file"
}

run_ecofloc_watch() {
  local rows_file=""
  local workers_file=""
  local pids_file=""
  local matches=""
  local status=""
  local last_render="0"
  local now="0"

  start_sudo_keepalive

  rows_file="$(mktemp)"
  workers_file="$(mktemp)"
  pids_file="$(mktemp)"

  GLOBAL_ROWS_FILE="$rows_file"
  GLOBAL_WORKERS_FILE="$workers_file"
  GLOBAL_PIDS_FILE="$pids_file"

  trap 'global_cleanup 0' EXIT
  trap handle_stop_signal INT TERM HUP QUIT TSTP

  hide_cursor
  clear_screen

  while true; do
    matches="$(get_matching_processes || true)"

    if [ -n "$matches" ]; then
      create_workers_for_matches "$matches" "$rows_file" "$workers_file" "$pids_file"
      status="monitoring"
    else
      status="waiting for matching processes"
    fi

    now="$(date +%s%N)"

    if [ "$last_render" = "0" ]; then
      render_ecofloc_frame "$rows_file" "$status"
      last_render="$now"
    else
      python3 - "$last_render" "$now" "$WATCH_INTERVAL" <<'PY'
import sys

last = int(sys.argv[1])
now = int(sys.argv[2])
interval = float(sys.argv[3])

elapsed = (now - last) / 1_000_000_000
sys.exit(0 if elapsed >= interval else 1)
PY
      if [ "$?" = "0" ]; then
        render_ecofloc_frame "$rows_file" "$status"
        last_render="$now"
      fi
    fi

    sleep "$SCAN_INTERVAL"
  done
}

run_ecofloc_once() {
  local rows_file=""
  local workers_file=""
  local pids_file=""
  local matches=""
  local active="0"

  start_sudo_keepalive

  rows_file="$(mktemp)"
  workers_file="$(mktemp)"
  pids_file="$(mktemp)"

  GLOBAL_ROWS_FILE="$rows_file"
  GLOBAL_WORKERS_FILE="$workers_file"
  GLOBAL_PIDS_FILE="$pids_file"

  trap 'global_cleanup 0' EXIT
  trap handle_stop_signal INT TERM HUP QUIT TSTP

  hide_cursor
  clear_screen

  matches="$(get_matching_processes || true)"

  if [ -z "$matches" ]; then
    echo "No matching Argo workflow process found."
    global_cleanup 0
  fi

  create_workers_for_matches "$matches" "$rows_file" "$workers_file" "$pids_file"

  while true; do
    active="$(wc -l < "$pids_file" | xargs)"

    render_ecofloc_frame "$rows_file" "monitoring; active metric workers: ${active}"

    if [ "$active" = "0" ]; then
      break
    fi

    sleep "$WATCH_INTERVAL"
  done

  render_ecofloc_frame "$rows_file" "done"
  global_cleanup 0
}

render_process_watch_frame() {
  local frame_file=""

  frame_file="$(mktemp)"

  {
    echo "Watching Argo workflow Python processes. Press Ctrl+C to stop."
    echo
    QUIET_HEADER=true run_once || true
  } > "$frame_file"

  move_home
  cat "$frame_file"
  clear_rest

  rm -f "$frame_file"
}

run_process_watch() {
  trap 'restore_terminal; exit 0' EXIT INT TERM HUP QUIT TSTP

  hide_cursor
  clear_screen

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

    --ecofloc-window)
      if [ $# -lt 2 ]; then
        echo "Missing value for --ecofloc-window"
        exit 1
      fi

      ECOFLOC_WINDOW="$2"
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

    --no-sudo-execute|--no-sudo)
      ECOFLOC_USE_SUDO_EXECUTE=false
      shift
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
      run_process_watch
    else
      run_once
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