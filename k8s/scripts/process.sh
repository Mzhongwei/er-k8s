#!/usr/bin/env bash

# Interact with processes running in the Argo workflow

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

# Server mode: commands run directly on the current machine.
COMMAND_WRAPPER=()

WATCH_INTERVAL="1"

ECOFLOC_INTERVAL="1000"
ECOFLOC_METRICS="cpu,ram,sd,nic,gpu"
ECOFLOC_EXPORT_PATH=""
ECOFLOC_USE_SUDO_EXECUTE=true
ECOFLOC_LOG_DIR="/tmp/erctl-ecofloc"

SUDO_KEEPALIVE_PID=""
GLOBAL_SESSIONS_FILE=""
GLOBAL_ROWS_FILE=""
CLEANUP_DONE=false
NEEDS_REDRAW=false

print_help() {
  cat <<'EOF'
Usage: process.sh [options] [command]

Commands:
  get                       Get the PID of the processes running in the Argo workflow.
  ecofloc                   Run EcoFloc on detected Argo workflow PIDs and show metrics as table columns.
                            EcoFloc follows each PID until it exits.

Options:
  --full                    Show full command output for matching processes.
  --watch                   Refresh continuously.
                            With get: refresh the process table.
                            With ecofloc: keep scanning for new PIDs and attach EcoFloc to them.
  --interval SECONDS        Refresh interval for --watch. Default: 1.

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
  process.sh --full get
  process.sh --watch get
  process.sh --watch --interval 0.5 get

  process.sh ecofloc
  process.sh ecofloc --metrics cpu,ram
  process.sh --watch ecofloc
EOF
}

run_wrapped() {
  "${COMMAND_WRAPPER[@]}" "$@"
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

restore_terminal() {
  tput cnorm 2>/dev/null || true
  tput rmcup 2>/dev/null || true
  stty sane 2>/dev/null || true
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

handle_resize_signal() {
  NEEDS_REDRAW=true
}

get_raw_processes() {
  run_wrapped ps axww -o pid=,args=
}

filter_processes() {
  python3 - <<'PY'
import os
import re
import sys

def parse_line(line):
    line = line.rstrip("\n")
    if not line.strip():
        return None, ""

    parts = line.strip().split(None, 1)

    if len(parts) == 1:
        return parts[0], ""

    return parts[0], parts[1]

def tokens(cmd):
    return cmd.split()

def base(path):
    return os.path.basename(path.rstrip())

def tok_cleanup(value):
    return value.strip().strip('"').strip("'")

def is_python(tok):
    b = base(tok)
    return (
        b == "python"
        or b == "python3"
        or b.startswith("python3.")
        or tok.endswith("/bin/python")
        or "/python" in tok
    )

def is_ignored(cmd):
    ignored = [
        "argoexec init",
        "argoexec wait",
        "python -u -m sidecar",
        "/usr/bin/python3 -sP /usr/bin/firewalld",
        "/usr/bin/python3 -Es /usr/sbin/tuned",
        "/usr/bin/python3 -Es /usr/sbin/tuned-ppd",
        "sudo /bin/execute ecofloc",
        "/opt/ecofloc/ecofloc",
        " ecofloc ",
    ]

    return any(x in cmd for x in ignored)

def is_candidate(cmd):
    if is_ignored(cmd):
        return False

    if "/app/" not in cmd:
        return False

    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if is_python(tok) and i + 1 < len(ts) and ts[i + 1].startswith("/app/"):
            return True

    if "argoexec emissary" in cmd:
        for i, tok in enumerate(ts):
            if tok == "--" and i + 2 < len(ts):
                if is_python(ts[i + 1]) and ts[i + 2].startswith("/app/"):
                    return True

    if re.search(r"(^|\s)(python|python3|/opt/venv/bin/python)\s+/app/", cmd):
        return True

    return False

def extract_script(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if is_python(tok) and i + 1 < len(ts):
            candidate = ts[i + 1]
            if candidate.startswith("/app/"):
                return base(candidate)

    for i, tok in enumerate(ts):
        if tok == "--" and i + 2 < len(ts):
            maybe_python = ts[i + 1]
            maybe_script = ts[i + 2]

            if is_python(maybe_python) and maybe_script.startswith("/app/"):
                return base(maybe_script)

    m = re.search(r"(?<!\S)/app/[^ ]+\.py(?!\S)", cmd)
    if m:
        return base(m.group(0))

    m = re.search(r"(?<!\S)/app/[^ ]+(?!\S)", cmd)
    if m:
        return base(m.group(0))

    return "-"

def extract_function(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if tok == "--function" and i + 1 < len(ts):
            return tok_cleanup(ts[i + 1])

        if tok.startswith("--function="):
            return tok_cleanup(tok.split("=", 1)[1])

    m = re.search(r"--function(?:=|\s+)([^ ]+)", cmd)
    if m:
        return tok_cleanup(m.group(1))

    return "-"

def score(cmd):
    value = 0

    if "/opt/venv/bin/python" in cmd:
        value += 100

    if re.search(r"(^|\s)python3?\s+/app/", cmd):
        value += 90

    if re.search(r"(^|\s)sh\s+-c\s+", cmd):
        value += 50

    if "argoexec emissary" in cmd:
        value += 30

    return value

selected = {}

try:
    for line in sys.stdin:
        pid, cmd = parse_line(line)

        if not pid or not cmd:
            continue

        if not is_candidate(cmd):
            continue

        script = extract_script(cmd)
        function = extract_function(cmd)

        key = (script, function)
        current_score = score(cmd)

        if key not in selected:
            selected[key] = (current_score, pid, cmd)
        else:
            old_score, _, _ = selected[key]
            if current_score > old_score:
                selected[key] = (current_score, pid, cmd)

    for _, pid, cmd in selected.values():
        print(f"{pid}\t{cmd}")

except KeyboardInterrupt:
    sys.exit(0)
PY
}

print_compact_table() {
  python3 - <<'PY'
import os
import re
import sys

PID_W = 8
SCRIPT_W = 40
FUNC_W = 28

def fit(value, width):
    value = str(value)
    if len(value) <= width:
        return value
    if width <= 1:
        return value[:width]
    return value[:width - 1] + "…"

print("{:<{}} | {:<{}} | {:<{}}".format(
    fit("PID", PID_W), PID_W,
    fit("SCRIPT", SCRIPT_W), SCRIPT_W,
    fit("FUNCTION", FUNC_W), FUNC_W
))
print("{}-+-{}-+-{}".format("-" * PID_W, "-" * SCRIPT_W, "-" * FUNC_W))

def parse_line(line):
    line = line.rstrip("\n")
    if "\t" not in line:
        return None, ""

    pid, cmd = line.split("\t", 1)
    return pid.strip(), cmd.strip()

def tokens(cmd):
    return cmd.split()

def base(path):
    return os.path.basename(path.rstrip())

def tok_cleanup(value):
    return value.strip().strip('"').strip("'")

def is_python(tok):
    b = base(tok)
    return (
        b == "python"
        or b == "python3"
        or b.startswith("python3.")
        or tok.endswith("/bin/python")
        or "/python" in tok
    )

def extract_script(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if is_python(tok) and i + 1 < len(ts):
            candidate = ts[i + 1]
            if candidate.startswith("/app/"):
                return base(candidate)

    for i, tok in enumerate(ts):
        if tok == "--" and i + 2 < len(ts):
            maybe_python = ts[i + 1]
            maybe_script = ts[i + 2]

            if is_python(maybe_python) and maybe_script.startswith("/app/"):
                return base(maybe_script)

    m = re.search(r"(?<!\S)/app/[^ ]+\.py(?!\S)", cmd)
    if m:
        return base(m.group(0))

    m = re.search(r"(?<!\S)/app/[^ ]+(?!\S)", cmd)
    if m:
        return base(m.group(0))

    return "-"

def extract_function(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if tok == "--function" and i + 1 < len(ts):
            return tok_cleanup(ts[i + 1])

        if tok.startswith("--function="):
            return tok_cleanup(tok.split("=", 1)[1])

    m = re.search(r"--function(?:=|\s+)([^ ]+)", cmd)
    if m:
        return tok_cleanup(m.group(1))

    return "-"

seen = set()

try:
    for line in sys.stdin:
        pid, cmd = parse_line(line)

        if not pid or not cmd:
            continue

        script = extract_script(cmd)
        function = extract_function(cmd)

        key = (script, function)

        if key in seen:
            continue

        seen.add(key)

        print("{:<{}} | {:<{}} | {:<{}}".format(
            fit(pid, PID_W), PID_W,
            fit(script, SCRIPT_W), SCRIPT_W,
            fit(function, FUNC_W), FUNC_W
        ))

except KeyboardInterrupt:
    sys.exit(0)
PY
}

print_ecofloc_metrics_table() {
  python3 - <<'PY'
import os
import re
import sys

PID_W = 8
SCRIPT_W = 30
FUNC_W = 24
METRIC_W = 15

def fit(value, width):
    value = str(value)
    if len(value) <= width:
        return value
    if width <= 1:
        return value[:width]
    return value[:width - 1] + "…"

def tokens(cmd):
    return cmd.split()

def base(path):
    return os.path.basename(path.rstrip())

def tok_cleanup(value):
    return value.strip().strip('"').strip("'")

def is_python(tok):
    b = base(tok)
    return (
        b == "python"
        or b == "python3"
        or b.startswith("python3.")
        or tok.endswith("/bin/python")
        or "/python" in tok
    )

def extract_script(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if is_python(tok) and i + 1 < len(ts):
            candidate = ts[i + 1]
            if candidate.startswith("/app/"):
                return base(candidate)

    for i, tok in enumerate(ts):
        if tok == "--" and i + 2 < len(ts):
            maybe_python = ts[i + 1]
            maybe_script = ts[i + 2]

            if is_python(maybe_python) and maybe_script.startswith("/app/"):
                return base(maybe_script)

    m = re.search(r"(?<!\S)/app/[^ ]+\.py(?!\S)", cmd)
    if m:
        return base(m.group(0))

    m = re.search(r"(?<!\S)/app/[^ ]+(?!\S)", cmd)
    if m:
        return base(m.group(0))

    return "-"

def extract_function(cmd):
    ts = tokens(cmd)

    for i, tok in enumerate(ts):
        if tok == "--function" and i + 1 < len(ts):
            return tok_cleanup(ts[i + 1])

        if tok.startswith("--function="):
            return tok_cleanup(tok.split("=", 1)[1])

    m = re.search(r"--function(?:=|\s+)([^ ]+)", cmd)
    if m:
        return tok_cleanup(m.group(1))

    return "-"

headers = ["PID", "SCRIPT", "FUNCTION", "CPU", "RAM", "SD", "NIC", "GPU"]
widths = [PID_W, SCRIPT_W, FUNC_W, METRIC_W, METRIC_W, METRIC_W, METRIC_W, METRIC_W]

print(" | ".join("{:<{}}".format(fit(h, w), w) for h, w in zip(headers, widths)))
print("-+-".join("-" * w for w in widths))

for line in sys.stdin:
    line = line.rstrip("\n")

    if not line:
        continue

    parts = line.split("\t")

    if len(parts) < 7:
        continue

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
PY
}

get_matching_processes() {
  get_raw_processes | filter_processes
}

run_once() {
  if [ "${QUIET_HEADER:-false}" != true ]; then
    echo "Getting PID of processes running in the Argo workflow..."
  fi

  if [ "$FULL" = true ]; then
    get_matching_processes
  else
    get_matching_processes | print_compact_table
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

start_ecofloc_for_pid_metric() {
  local pid="$1"
  local metric="$2"
  local log_file="$3"
  local export_arg=()

  if [ -n "$ECOFLOC_EXPORT_PATH" ]; then
    export_arg=(-f "$ECOFLOC_EXPORT_PATH")
  fi

  mkdir -p "$ECOFLOC_LOG_DIR"

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

  # Try graceful stop on the whole process group first.
  kill -INT "-${eco_pid}" 2>/dev/null || true
  kill -INT "$eco_pid" 2>/dev/null || true

  sleep 1

  # If still alive, escalate.
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
    echo "N/A"
    return 0
  fi

  local avg=""
  local total=""

  # Use the LAST summary values, not the first.
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
      echo "N/A"
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

write_ecofloc_rows_file() {
  local file="$1"
  local rows="$2"

  printf "%s" "$rows" > "$file"
}

update_ecofloc_cell() {
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

bad_values = {"N/A", "ERR", "CMD_ERR", ""}
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

        # Never overwrite a valid finalized measurement with N/A/ERR/CMD_ERR.
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

refresh_running_ecofloc_cells() {
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
      update_ecofloc_cell "$rows_file" "$pid" "$metric" "RUN ${elapsed}s"
    fi
  done < "$sessions_file"
}

append_ecofloc_row_if_missing() {
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

build_initial_ecofloc_rows() {
  local matches="$1"
  local rows=""
  local cpu="N/A"
  local ram="N/A"
  local sd="N/A"
  local nic="N/A"
  local gpu="N/A"

  while IFS=$'\t' read -r pid cmd; do
    [ -n "${pid:-}" ] || continue
    [ -n "${cmd:-}" ] || continue

    cpu="N/A"
    ram="N/A"
    sd="N/A"
    nic="N/A"
    gpu="N/A"

    if metric_enabled "cpu"; then cpu="WAIT"; fi
    if metric_enabled "ram"; then ram="WAIT"; fi
    if metric_enabled "sd"; then sd="WAIT"; fi
    if metric_enabled "nic"; then nic="WAIT"; fi
    if metric_enabled "gpu"; then gpu="WAIT"; fi

    rows+="${pid}"$'\t'"${cmd}"$'\t'"${cpu}"$'\t'"${ram}"$'\t'"${sd}"$'\t'"${nic}"$'\t'"${gpu}"$'\n'
  done <<< "$matches"

  printf "%s" "$rows"
}

render_ecofloc_content() {
  local rows_file="$1"
  local status="${2:-}"

  {
    echo "EcoFloc PID-lifetime monitoring. Press Ctrl+C to stop."

    if [ -n "$status" ]; then
      echo "Status: ${status}"
    fi

    echo

    if [ -s "$rows_file" ]; then
      cat "$rows_file" | print_ecofloc_metrics_table
    else
      echo "No matching Argo workflow process found."
    fi
  }
}

render_ecofloc_frame() {
  local rows_file="$1"
  local status="${2:-}"
  local frame_file

  frame_file="$(mktemp)"
  render_ecofloc_content "$rows_file" "$status" > "$frame_file"

  tput cup 0 0 2>/dev/null || printf "\033[H"
  cat "$frame_file"
  tput ed 2>/dev/null || printf "\033[J"

  rm -f "$frame_file"
  NEEDS_REDRAW=false
}

create_ecofloc_sessions_for_matches() {
  local matches="$1"
  local rows_file="$2"
  local sessions_file="$3"
  local session_id
  local metric
  local log_file
  local eco_pid
  local start_ts

  session_id="$(date +%s%N)"

  ensure_ecofloc_log_dir

  while IFS=$'\t' read -r pid cmd; do
    [ -n "${pid:-}" ] || continue
    [ -n "${cmd:-}" ] || continue

    append_ecofloc_row_if_missing "$rows_file" "$pid" "$cmd"

    IFS=',' read -r -a metric_list <<< "$ECOFLOC_METRICS"

    for metric in "${metric_list[@]}"; do
      metric="$(printf "%s" "$metric" | tr "[:upper:]" "[:lower:]" | xargs)"

      case "$metric" in
        cpu|ram|sd|nic|gpu)
          if grep -q "^${pid}"$'\t'"${metric}"$'\t' "$sessions_file" 2>/dev/null; then
            continue
          fi

          start_ts="$(date +%s)"
          update_ecofloc_cell "$rows_file" "$pid" "$metric" "RUN 0s"

          log_file="${ECOFLOC_LOG_DIR}/ecofloc_${session_id}_${pid}_${metric}.log"
          eco_pid="$(start_ecofloc_for_pid_metric "$pid" "$metric" "$log_file" | tail -n 1 | tr -d '\r' | xargs)"

          if [ -z "$eco_pid" ]; then
            update_ecofloc_cell "$rows_file" "$pid" "$metric" "ERR"
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
    sleep 1

    result="$(parse_ecofloc_log "$log_file" | tail -n 1 | tr -d '\r')"

    if [ -z "$result" ]; then
      result="N/A"
    fi

    update_ecofloc_cell "$rows_file" "$pid" "$metric" "$result"
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
    sleep 1

    if [ -n "$rows_file" ] && [ -f "$rows_file" ]; then
      result="$(parse_ecofloc_log "$log_file" | tail -n 1 | tr -d '\r')"

      if [ -z "$result" ]; then
        result="N/A"
      fi

      update_ecofloc_cell "$rows_file" "$pid" "$metric" "$result"
    fi
  done < "$sessions_file"

  : > "$sessions_file"
}

run_ecofloc_once() {
  local matches
  local rows_file
  local sessions_file
  local initial_rows
  local active_sessions

  start_sudo_keepalive

  rows_file="$(mktemp)"
  sessions_file="$(mktemp)"
  GLOBAL_ROWS_FILE="$rows_file"
  GLOBAL_SESSIONS_FILE="$sessions_file"

  trap 'global_cleanup 0' EXIT
  trap handle_stop_signal INT TERM HUP QUIT TSTP
  trap handle_resize_signal WINCH

  matches="$(get_matching_processes || true)"

  if [ -z "$matches" ]; then
    echo "No matching Argo workflow process found."
    global_cleanup 0
  fi

  initial_rows="$(build_initial_ecofloc_rows "$matches")"
  write_ecofloc_rows_file "$rows_file" "$initial_rows"

  create_ecofloc_sessions_for_matches "$matches" "$rows_file" "$sessions_file"

  clear
  echo "EcoFloc attached to detected PIDs. Waiting until they exit..."
  echo
  cat "$rows_file" | print_ecofloc_metrics_table

  while true; do
    refresh_running_ecofloc_cells "$rows_file" "$sessions_file"
    finalize_finished_sessions "$rows_file" "$sessions_file"

    clear
    echo "EcoFloc attached to detected PIDs. Waiting until they exit..."
    echo
    cat "$rows_file" | print_ecofloc_metrics_table

    active_sessions="$(wc -l < "$sessions_file" | xargs)"

    if [ "$active_sessions" = "0" ]; then
      break
    fi

    sleep "$WATCH_INTERVAL"
  done

  clear
  cat "$rows_file" | print_ecofloc_metrics_table

  global_cleanup 0
}

run_ecofloc_watch() {
  local rows_file
  local sessions_file
  local matches
  local status="initializing"
  local active_sessions

  start_sudo_keepalive

  rows_file="$(mktemp)"
  sessions_file="$(mktemp)"
  GLOBAL_ROWS_FILE="$rows_file"
  GLOBAL_SESSIONS_FILE="$sessions_file"

  trap 'global_cleanup 0' EXIT
  trap handle_stop_signal INT TERM HUP QUIT TSTP
  trap handle_resize_signal WINCH

  clear
  tput smcup 2>/dev/null || true
  tput civis 2>/dev/null || true
  tput clear 2>/dev/null || clear

  while true; do
    matches="$(get_matching_processes || true)"

    if [ -n "$matches" ]; then
      create_ecofloc_sessions_for_matches "$matches" "$rows_file" "$sessions_file"
      status="monitoring"
    else
      status="waiting for matching processes"
    fi

    refresh_running_ecofloc_cells "$rows_file" "$sessions_file"
    finalize_finished_sessions "$rows_file" "$sessions_file"

    active_sessions="$(wc -l < "$sessions_file" | xargs)"

    if [ "$active_sessions" != "0" ]; then
      status="monitoring; active EcoFloc sessions: ${active_sessions}"
    fi

    render_ecofloc_frame "$rows_file" "$status"

    sleep "$WATCH_INTERVAL"
  done
}

render_process_watch_content() {
  {
    echo "Watching Argo workflow processes. Press Ctrl+C to stop."
    echo
    QUIET_HEADER=true run_once || true
  }
}

run_watch() {
  local frame_file

  cleanup_process_watch() {
    restore_terminal
    exit 0
  }

  trap cleanup_process_watch EXIT INT TERM HUP QUIT TSTP
  trap handle_resize_signal WINCH

  clear
  tput smcup 2>/dev/null || true
  tput civis 2>/dev/null || true
  tput clear 2>/dev/null || clear

  while true; do
    frame_file="$(mktemp)"
    render_process_watch_content > "$frame_file"

    tput cup 0 0 2>/dev/null || printf "\033[H"
    cat "$frame_file"
    tput ed 2>/dev/null || printf "\033[J"

    rm -f "$frame_file"
    NEEDS_REDRAW=false

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
      run_watch
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