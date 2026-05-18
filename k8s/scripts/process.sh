#!/usr/bin/env bash

# Interact with processes running in the Argo workflow

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

# Server mode: commands run directly on the current machine.
COMMAND_WRAPPER=()

WATCH_INTERVAL="1"
SCAN_INTERVAL="0.2"

ECOFLOC_INTERVAL="1000"
ECOFLOC_METRICS="cpu,ram,sd,nic,gpu"
ECOFLOC_EXPORT_PATH=""
ECOFLOC_USE_SUDO_EXECUTE=true
ECOFLOC_LOG_DIR="/tmp/erctl-ecofloc"

SUDO_KEEPALIVE_PID=""
GLOBAL_SESSIONS_FILE=""
GLOBAL_ROWS_FILE=""
CLEANUP_DONE=false

print_help() {
  cat <<'EOF'
Usage: process.sh [options] [command]

Commands:
  get                       Get the PID of Python processes running in the Argo workflow.
  ecofloc                   Run EcoFloc on detected Argo workflow PIDs and show metrics as table columns.
                            EcoFloc follows each PID until it exits.

Options:
  --full                    Show full command output for matching processes.
  --watch                   Refresh continuously.
                            With get: refresh the process table.
                            With ecofloc: keep scanning for new PIDs and attach EcoFloc to them.
  --interval SECONDS        Display refresh interval. Default: 1.
  --scan-interval SECONDS   Process detection interval for ecofloc watch. Default: 0.2.

EcoFloc options:
  --ecofloc-interval MS     EcoFloc measurement interval in milliseconds. Default: 1000.
  --metrics LIST            Comma-separated metrics. Default: cpu,ram,sd,nic,gpu.
                            Example: --metrics cpu,ram
  --export PATH             Pass -f PATH to EcoFloc for CSV export.
  --no-sudo-execute         Run ecofloc directly instead of sudo /bin/execute ecofloc.

Other:
  --help                    Show this help.

Examples:
  process.sh get
  process.sh --watch get
  process.sh ecofloc
  process.sh --watch ecofloc
  process.sh --watch ecofloc --metrics cpu,ram
EOF
}

run_wrapped() {
  "${COMMAND_WRAPPER[@]}" "$@"
}

clear_screen() {
  printf '\033[2J\033[H'
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

global_cleanup() {
  local exit_code="${1:-0}"

  if [ "$CLEANUP_DONE" = true ]; then
    exit "$exit_code"
  fi

  CLEANUP_DONE=true

  if [ -n "${GLOBAL_SESSIONS_FILE:-}" ] && [ -f "$GLOBAL_SESSIONS_FILE" ]; then
    stop_all_ecofloc_sessions "$GLOBAL_SESSIONS_FILE" "${GLOBAL_ROWS_FILE:-}" >/dev/null 2>&1 || true
  fi

  stop_sudo_keepalive >/dev/null 2>&1 || true
  restore_terminal

  if [ -n "${GLOBAL_ROWS_FILE:-}" ] && [ -f "$GLOBAL_ROWS_FILE" ]; then
    rm -f "$GLOBAL_ROWS_FILE" >/dev/null 2>&1 || true
  fi

  if [ -n "${GLOBAL_SESSIONS_FILE:-}" ] && [ -f "$GLOBAL_SESSIONS_FILE" ]; then
    rm -f "$GLOBAL_SESSIONS_FILE" >/dev/null 2>&1 || true
  fi

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
        or b.endswith("python")
        or b.startswith("python")
        or tok.endswith("/bin/python")
        or tok.endswith("/bin/python3")
        or "/python" in tok
    )

def script_after_python(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if is_python_token(tok) and i + 1 < len(ts):
            candidate = ts[i + 1]
            if candidate.startswith("/app/"):
                return candidate

    return ""

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
    if not script:
        return False

    # Keep actual Python workers only.
    # This includes:
    #   python /app/producer.py
    #   /opt/venv/bin/python /app/distributions/foo.py
    return script.startswith("/app/")

seen_pids = set()

for line in sys.stdin:
    pid, cmd = parse_line(line)

    if not pid or not cmd:
        continue

    if pid in seen_pids:
        continue

    if not is_target(cmd):
        continue

    seen_pids.add(pid)
    print(f"{pid}\t{cmd}")
'
}

extract_process_table() {
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

def is_python_token(tok):
    b = base(tok)
    return (
        b == "python"
        or b == "python3"
        or b.startswith("python3.")
        or b.endswith("python")
        or b.startswith("python")
        or tok.endswith("/bin/python")
        or tok.endswith("/bin/python3")
        or "/python" in tok
    )

def extract_script(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if is_python_token(tok) and i + 1 < len(ts):
            candidate = ts[i + 1]
            if candidate.startswith("/app/"):
                return base(candidate)

    m = re.search(r"(?<!\\S)/app/[^ ]+\\.py(?!\\S)", cmd)
    if m:
        return base(m.group(0))

    m = re.search(r"(?<!\\S)/app/[^ ]+(?!\\S)", cmd)
    if m:
        return base(m.group(0))

    return "-"

def extract_function(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if tok == "--function" and i + 1 < len(ts):
            return clean(ts[i + 1])

        if tok.startswith("--function="):
            return clean(tok.split("=", 1)[1])

    m = re.search(r"--function(?:=|\\s+)([^ ]+)", cmd)
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
    return value[:max(width - 1, 0)] + "…"

def base(path):
    return os.path.basename(path.rstrip())

def tokens(cmd):
    return cmd.split()

def clean(value):
    return value.strip().strip("\"").strip(chr(39))

def is_python_token(tok):
    b = base(tok)
    return (
        b == "python"
        or b == "python3"
        or b.startswith("python3.")
        or b.endswith("python")
        or b.startswith("python")
        or tok.endswith("/bin/python")
        or tok.endswith("/bin/python3")
        or "/python" in tok
    )

def extract_script(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if is_python_token(tok) and i + 1 < len(ts):
            candidate = ts[i + 1]
            if candidate.startswith("/app/"):
                return base(candidate)

    m = re.search(r"(?<!\\S)/app/[^ ]+\\.py(?!\\S)", cmd)
    if m:
        return base(m.group(0))

    m = re.search(r"(?<!\\S)/app/[^ ]+(?!\\S)", cmd)
    if m:
        return base(m.group(0))

    return "-"

def extract_function(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if tok == "--function" and i + 1 < len(ts):
            return clean(ts[i + 1])

        if tok.startswith("--function="):
            return clean(tok.split("=", 1)[1])

    m = re.search(r"--function(?:=|\\s+)([^ ]+)", cmd)
    if m:
        return clean(m.group(1))

    return "-"

headers = ["PID", "SCRIPT", "FUNCTION", "CPU", "RAM", "SD", "NIC", "GPU"]
widths = [PID_W, SCRIPT_W, FUNC_W, METRIC_W, METRIC_W, METRIC_W, METRIC_W, METRIC_W]

print(" | ".join("{:<{}}".format(h, w) for h, w in zip(headers, widths)))
print("-+-".join("-" * w for w in widths))

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t")

    while len(parts) < 7:
        parts.append("N/A")

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

    print(" | ".join("{:<{}}".format(fit(v, w), w) for v, w in zip(values, widths)))
'
}

get_matching_processes() {
  get_raw_processes | filter_processes
}

run_once() {
  if [ "${QUIET_HEADER:-false}" != true ]; then
    echo "Getting PID of Python processes running in the Argo workflow..."
  fi

  if [ "$FULL" = true ]; then
    get_matching_processes
  else
    get_matching_processes | extract_process_table
  fi
}

metric_enabled() {
  local target="$1"
  local metric

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

start_ecofloc_for_pid_metric() {
  local pid="$1"
  local metric="$2"
  local log_file="$3"
  local export_arg=()

  if [ -n "$ECOFLOC_EXPORT_PATH" ]; then
    export_arg=(-f "$ECOFLOC_EXPORT_PATH")
  fi

  ensure_ecofloc_log_dir

  if command -v setsid >/dev/null 2>&1; then
    if [ "$ECOFLOC_USE_SUDO_EXECUTE" = true ]; then
      setsid sudo /bin/execute ecofloc "--${metric}" \
        -p "$pid" \
        -i "$ECOFLOC_INTERVAL" \
        -t -1 \
        "${export_arg[@]}" > "$log_file" 2>&1 &
    else
      setsid ecofloc "--${metric}" \
        -p "$pid" \
        -i "$ECOFLOC_INTERVAL" \
        -t -1 \
        "${export_arg[@]}" > "$log_file" 2>&1 &
    fi
  else
    if [ "$ECOFLOC_USE_SUDO_EXECUTE" = true ]; then
      sudo /bin/execute ecofloc "--${metric}" \
        -p "$pid" \
        -i "$ECOFLOC_INTERVAL" \
        -t -1 \
        "${export_arg[@]}" > "$log_file" 2>&1 &
    else
      ecofloc "--${metric}" \
        -p "$pid" \
        -i "$ECOFLOC_INTERVAL" \
        -t -1 \
        "${export_arg[@]}" > "$log_file" 2>&1 &
    fi
  fi

  echo $!
}

stop_ecofloc() {
  local eco_pid="$1"

  if [ -z "$eco_pid" ]; then
    return 0
  fi

  kill -INT "-${eco_pid}" 2>/dev/null || true
  kill -INT "$eco_pid" 2>/dev/null || true

  sleep 1

  if kill -0 "$eco_pid" 2>/dev/null; then
    kill -TERM "-${eco_pid}" 2>/dev/null || true
    kill -TERM "$eco_pid" 2>/dev/null || true
  fi

  sleep 1

  if kill -0 "$eco_pid" 2>/dev/null; then
    kill -KILL "-${eco_pid}" 2>/dev/null || true
    kill -KILL "$eco_pid" 2>/dev/null || true
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
      /Total Energy/ {
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

bad_values = {"N/A", "", "ERR", "CMD_ERR", "NO_DATA"}
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
        parts.append("N/A")

    if parts[0] == target_pid:
        old_value = parts[idx]

        old_is_final = bool(final_re.match(old_value))
        new_is_bad = new_value in bad_values

        # Never overwrite a valid result with an error/no-data value.
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

cpu = "WAIT" if "cpu" in metrics else "N/A"
ram = "WAIT" if "ram" in metrics else "N/A"
sd = "WAIT" if "sd" in metrics else "N/A"
nic = "WAIT" if "nic" in metrics else "N/A"
gpu = "WAIT" if "gpu" in metrics else "N/A"

rows.append("\t".join([pid, cmd, cpu, ram, sd, nic, gpu]))

with open(path, "w", encoding="utf-8") as f:
    f.write("\n".join(rows) + "\n")
PY
}

refresh_running_cells() {
  local rows_file="$1"
  local sessions_file="$2"
  local now
  local pid
  local metric
  local eco_pid
  local log_file
  local start_ts
  local elapsed

  now="$(date +%s)"

  if [ ! -s "$sessions_file" ]; then
    return 0
  fi

  while IFS=$'\t' read -r pid metric eco_pid log_file start_ts; do
    [ -n "${pid:-}" ] || continue
    [ -n "${metric:-}" ] || continue
    [ -n "${start_ts:-}" ] || continue

    if pid_alive "$pid"; then
      elapsed="$((now - start_ts))"
      safe_update_cell "$rows_file" "$pid" "$metric" "RUN ${elapsed}s"
    fi
  done < "$sessions_file"
}

create_sessions_for_matches() {
  local matches="$1"
  local rows_file="$2"
  local sessions_file="$3"
  local session_id
  local pid
  local cmd
  local metric
  local log_file
  local eco_pid
  local start_ts

  session_id="$(date +%s%N)"
  ensure_ecofloc_log_dir

  while IFS=$'\t' read -r pid cmd; do
    [ -n "${pid:-}" ] || continue
    [ -n "${cmd:-}" ] || continue

    append_row_if_missing "$rows_file" "$pid" "$cmd"

    IFS=',' read -r -a metric_list <<< "$ECOFLOC_METRICS"

    for metric in "${metric_list[@]}"; do
      metric="$(printf "%s" "$metric" | tr "[:upper:]" "[:lower:]" | xargs)"

      case "$metric" in
        cpu|ram|sd|nic|gpu)
          if grep -q "^${pid}"$'\t'"${metric}"$'\t' "$sessions_file" 2>/dev/null; then
            continue
          fi

          start_ts="$(date +%s)"
          safe_update_cell "$rows_file" "$pid" "$metric" "RUN 0s"

          log_file="${ECOFLOC_LOG_DIR}/ecofloc_${session_id}_${pid}_${metric}.log"
          eco_pid="$(start_ecofloc_for_pid_metric "$pid" "$metric" "$log_file" | tail -n 1 | tr -d '\r' | xargs)"

          if [ -z "$eco_pid" ]; then
            safe_update_cell "$rows_file" "$pid" "$metric" "ERR"
            continue
          fi

          printf "%s\t%s\t%s\t%s\t%s\n" "$pid" "$metric" "$eco_pid" "$log_file" "$start_ts" >> "$sessions_file"
          ;;
        *)
          ;;
      esac
    done
  done <<< "$matches"
}

finalize_finished_sessions() {
  local rows_file="$1"
  local sessions_file="$2"
  local remaining_file
  local pid
  local metric
  local eco_pid
  local log_file
  local start_ts
  local result

  remaining_file="$(mktemp)"

  if [ ! -s "$sessions_file" ]; then
    : > "$remaining_file"
    mv "$remaining_file" "$sessions_file"
    return 0
  fi

  while IFS=$'\t' read -r pid metric eco_pid log_file start_ts; do
    [ -n "${pid:-}" ] || continue
    [ -n "${metric:-}" ] || continue
    [ -n "${eco_pid:-}" ] || continue
    [ -n "${log_file:-}" ] || continue

    if pid_alive "$pid"; then
      printf "%s\t%s\t%s\t%s\t%s\n" "$pid" "$metric" "$eco_pid" "$log_file" "$start_ts" >> "$remaining_file"
      continue
    fi

    stop_ecofloc "$eco_pid"
    result="$(parse_ecofloc_log "$log_file" | tail -n 1 | tr -d '\r')"

    if [ -z "$result" ]; then
      result="NO_DATA"
    fi

    safe_update_cell "$rows_file" "$pid" "$metric" "$result"
  done < "$sessions_file"

  mv "$remaining_file" "$sessions_file"
}

stop_all_ecofloc_sessions() {
  local sessions_file="$1"
  local rows_file="${2:-}"
  local pid
  local metric
  local eco_pid
  local log_file
  local start_ts
  local result

  if [ ! -s "$sessions_file" ]; then
    return 0
  fi

  while IFS=$'\t' read -r pid metric eco_pid log_file start_ts; do
    [ -n "${pid:-}" ] || continue
    [ -n "${metric:-}" ] || continue
    [ -n "${eco_pid:-}" ] || continue
    [ -n "${log_file:-}" ] || continue

    stop_ecofloc "$eco_pid"

    if [ -n "$rows_file" ] && [ -f "$rows_file" ]; then
      result="$(parse_ecofloc_log "$log_file" | tail -n 1 | tr -d '\r')"

      if [ -z "$result" ]; then
        result="NO_DATA"
      fi

      safe_update_cell "$rows_file" "$pid" "$metric" "$result"
    fi
  done < "$sessions_file"

  : > "$sessions_file"
}

render_ecofloc_frame() {
  local rows_file="$1"
  local status="${2:-}"
  local frame_file

  frame_file="$(mktemp)"

  {
    echo "EcoFloc PID monitoring. Press Ctrl+C to stop."

    if [ -n "$status" ]; then
      echo "Status: ${status}"
    fi

    echo

    if [ -s "$rows_file" ]; then
      cat "$rows_file" | print_ecofloc_table
    else
      echo "No matching Argo workflow process found."
    fi
  } > "$frame_file"

  printf '\033[H'
  cat "$frame_file"
  printf '\033[J'

  rm -f "$frame_file"
}

run_ecofloc_watch() {
  local rows_file
  local sessions_file
  local matches
  local status
  local active_sessions
  local last_render
  local now

  start_sudo_keepalive

  rows_file="$(mktemp)"
  sessions_file="$(mktemp)"

  GLOBAL_ROWS_FILE="$rows_file"
  GLOBAL_SESSIONS_FILE="$sessions_file"

  trap 'global_cleanup 0' EXIT
  trap handle_stop_signal INT TERM HUP QUIT TSTP

  hide_cursor
  clear_screen

  last_render=0

  while true; do
    matches="$(get_matching_processes || true)"

    if [ -n "$matches" ]; then
      create_sessions_for_matches "$matches" "$rows_file" "$sessions_file"
      status="monitoring"
    else
      status="waiting for matching processes"
    fi

    refresh_running_cells "$rows_file" "$sessions_file"
    finalize_finished_sessions "$rows_file" "$sessions_file"

    active_sessions="$(wc -l < "$sessions_file" | xargs)"

    if [ "$active_sessions" != "0" ]; then
      status="monitoring; active EcoFloc sessions: ${active_sessions}"
    fi

    now="$(date +%s)"

    if [ "$((now - last_render))" -ge "${WATCH_INTERVAL%.*}" ] || [ "$last_render" = "0" ]; then
      render_ecofloc_frame "$rows_file" "$status"
      last_render="$now"
    fi

    sleep "$SCAN_INTERVAL"
  done
}

run_ecofloc_once() {
  local rows_file
  local sessions_file
  local matches
  local active_sessions

  start_sudo_keepalive

  rows_file="$(mktemp)"
  sessions_file="$(mktemp)"

  GLOBAL_ROWS_FILE="$rows_file"
  GLOBAL_SESSIONS_FILE="$sessions_file"

  trap 'global_cleanup 0' EXIT
  trap handle_stop_signal INT TERM HUP QUIT TSTP

  hide_cursor
  clear_screen

  matches="$(get_matching_processes || true)"

  if [ -z "$matches" ]; then
    echo "No matching Argo workflow process found."
    global_cleanup 0
  fi

  create_sessions_for_matches "$matches" "$rows_file" "$sessions_file"

  while true; do
    refresh_running_cells "$rows_file" "$sessions_file"
    finalize_finished_sessions "$rows_file" "$sessions_file"

    active_sessions="$(wc -l < "$sessions_file" | xargs)"

    render_ecofloc_frame "$rows_file" "monitoring; active EcoFloc sessions: ${active_sessions}"

    if [ "$active_sessions" = "0" ]; then
      break
    fi

    sleep "$WATCH_INTERVAL"
  done

  render_ecofloc_frame "$rows_file" "done"
  global_cleanup 0
}

render_process_watch_frame() {
  local frame_file

  frame_file="$(mktemp)"

  {
    echo "Watching Argo workflow Python processes. Press Ctrl+C to stop."
    echo
    QUIET_HEADER=true run_once || true
  } > "$frame_file"

  printf '\033[H'
  cat "$frame_file"
  printf '\033[J'

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

    --ecofloc-duration)
      echo "--ecofloc-duration is ignored in PID-lifetime mode. EcoFloc uses -t -1 and stops when the target PID exits."
      shift
      if [ $# -gt 0 ] && [[ "$1" != -* ]]; then
        shift
      fi
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