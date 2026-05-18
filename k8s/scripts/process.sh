#!/usr/bin/env bash

# Interact with processes running in the Argo workflow

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

PROFILE="domolandes"
COMMAND_WRAPPER=()

WATCH_INTERVAL="1"

ECOFLOC_INTERVAL="1000"
ECOFLOC_DURATION="10"
ECOFLOC_METRICS="cpu,ram,sd,nic,gpu"
ECOFLOC_EXPORT_PATH=""
ECOFLOC_USE_SUDO=true

print_help() {
  cat <<'EOF'
Usage: process.sh [options] [command]

Commands:
  get                       Get the PID of the processes running in the Argo workflow.
  ecofloc                   Run EcoFloc on detected Argo workflow PIDs for all metrics.

Options:
  --full                    Show full command output for matching processes.
  --watch                   Refresh the output continuously.
  --interval SECONDS        Refresh interval for --watch. Default: 1.

EcoFloc options:
  --ecofloc-interval MS     EcoFloc measurement interval in milliseconds. Default: 1000.
  --ecofloc-duration SEC    EcoFloc measurement duration in seconds. Default: 10.
  --metrics LIST            Comma-separated metrics. Default: cpu,ram,sd,nic,gpu.
                            Example: --metrics cpu,ram
  --export PATH             Pass -f PATH to EcoFloc for CSV export.
  --no-sudo                 Run ecofloc without sudo inside Minikube.

Other:
  --help                    Show this help.

Examples:
  process.sh get
  process.sh --full get
  process.sh --watch get
  process.sh --watch --interval 0.5 get

  process.sh ecofloc
  process.sh ecofloc --ecofloc-duration 5
  process.sh ecofloc --metrics cpu,ram
  process.sh ecofloc --export /tmp/ecofloc
  process.sh --watch ecofloc --ecofloc-duration 2
EOF
}

get_raw_processes() {
  # Output format:
  # PID COMMAND
  #
  # axww prevents command truncation.
  "${COMMAND_WRAPPER[@]}" ps axww -o pid=,args=
}

filter_processes() {
  python3 -c '
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
    return value.strip().strip("\"")

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
    ]

    return any(x in cmd for x in ignored)

def is_candidate(cmd):
    if is_ignored(cmd):
        return False

    if "/app/" not in cmd:
        return False

    ts = tokens(cmd)

    # Normal case:
    # /opt/venv/bin/python /app/distributions/file.py ...
    for i, tok in enumerate(ts):
        if is_python(tok) and i + 1 < len(ts) and ts[i + 1].startswith("/app/"):
            return True

    # Argo emissary case:
    # argoexec emissary ... -- /opt/venv/bin/python /app/distributions/file.py ...
    if "argoexec emissary" in cmd:
        for i, tok in enumerate(ts):
            if tok == "--" and i + 2 < len(ts):
                if is_python(ts[i + 1]) and ts[i + 2].startswith("/app/"):
                    return True

    # Shell wrapper case:
    # sh -c /opt/venv/bin/python /app/distributions/file.py ...
    if re.search(r"(^|\s)(python|python3|/opt/venv/bin/python)\s+/app/", cmd):
        return True

    return False

def extract_script(cmd):
    ts = tokens(cmd)

    # Normal Python command.
    for i, tok in enumerate(ts):
        if is_python(tok) and i + 1 < len(ts):
            candidate = ts[i + 1]
            if candidate.startswith("/app/"):
                return base(candidate)

    # Argo emissary command.
    for i, tok in enumerate(ts):
        if tok == "--" and i + 2 < len(ts):
            maybe_python = ts[i + 1]
            maybe_script = ts[i + 2]

            if is_python(maybe_python) and maybe_script.startswith("/app/"):
                return base(maybe_script)

    # Fallback: any /app/*.py path.
    m = re.search(r"(?<!\S)/app/[^ ]+\.py(?!\S)", cmd)
    if m:
        return base(m.group(0))

    # Fallback: /app/name without .py.
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

    # Prefer real worker process.
    if "/opt/venv/bin/python" in cmd:
        value += 100

    if re.search(r"(^|\s)python3?\s+/app/", cmd):
        value += 90

    # Shell wrapper is useful but less precise.
    if re.search(r"(^|\s)sh\s+-c\s+", cmd):
        value += 50

    # Emissary wrapper is useful during startup but not preferred.
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

        # Avoid doubles.
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
'
}

print_compact_table() {
  python3 -c '
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
    return value.strip().strip("\"")

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
'
}

print_ecofloc_target_table() {
  python3 -c '
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
    return value.strip().strip("\"")

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

print("{:<{}} | {:<{}} | {:<{}}".format(
    fit("PID", PID_W), PID_W,
    fit("SCRIPT", SCRIPT_W), SCRIPT_W,
    fit("FUNCTION", FUNC_W), FUNC_W
))
print("{}-+-{}-+-{}".format("-" * PID_W, "-" * SCRIPT_W, "-" * FUNC_W))

for line in sys.stdin:
    pid, cmd = parse_line(line)

    if not pid or not cmd:
        continue

    print("{:<{}} | {:<{}} | {:<{}}".format(
        fit(pid, PID_W), PID_W,
        fit(extract_script(cmd), SCRIPT_W), SCRIPT_W,
        fit(extract_function(cmd), FUNC_W), FUNC_W
    ))
'
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

run_ecofloc_for_pid_metric() {
  local pid="$1"
  local metric="$2"

  local component="--${metric}"
  local cmd_prefix=()

  if [ "$ECOFLOC_USE_SUDO" = true ]; then
    cmd_prefix=(sudo execute)
  fi

  echo
  echo "================================================================"
  echo "EcoFloc metric: ${metric}"
  echo "PID: ${pid}"
  echo "Interval: ${ECOFLOC_INTERVAL} ms"
  echo "Duration: ${ECOFLOC_DURATION} s"
  echo "================================================================"

  if [ -n "$ECOFLOC_EXPORT_PATH" ]; then
    "${COMMAND_WRAPPER[@]}" "${cmd_prefix[@]}" ecofloc "$component" -p "$pid" -i "$ECOFLOC_INTERVAL" -t "$ECOFLOC_DURATION" -f "$ECOFLOC_EXPORT_PATH" || {
      echo "EcoFloc failed for PID=${pid}, metric=${metric}"
      return 0
    }
  else
    "${COMMAND_WRAPPER[@]}" "${cmd_prefix[@]}" ecofloc "$component" -p "$pid" -i "$ECOFLOC_INTERVAL" -t "$ECOFLOC_DURATION" || {
      echo "EcoFloc failed for PID=${pid}, metric=${metric}"
      return 0
    }
  fi
}

run_ecofloc_once() {
  local matches
  matches="$(get_matching_processes || true)"

  if [ -z "$matches" ]; then
    echo "No matching Argo workflow process found."
    return 0
  fi

  echo "Detected Argo workflow processes:"
  printf '%s\n' "$matches" | print_ecofloc_target_table
  echo

  IFS=',' read -r -a metrics <<< "$ECOFLOC_METRICS"

  while IFS=$'\t' read -r pid cmd; do
    [ -n "${pid:-}" ] || continue

    for metric in "${metrics[@]}"; do
      metric="$(printf '%s' "$metric" | tr '[:upper:]' '[:lower:]' | xargs)"

      case "$metric" in
        cpu|ram|sd|nic|gpu)
          run_ecofloc_for_pid_metric "$pid" "$metric"
          ;;
        *)
          echo "Skipping unknown EcoFloc metric: $metric"
          ;;
      esac
    done
  done <<< "$matches"
}

run_watch() {
  cleanup_watch() {
    tput cnorm 2>/dev/null || true
    tput rmcup 2>/dev/null || true
    printf "\nStopped watching.\n"
    exit 0
  }

  trap cleanup_watch INT TERM

  tput smcup 2>/dev/null || true
  tput civis 2>/dev/null || true

  while true; do
    tput cup 0 0 2>/dev/null || printf "\033[H"
    tput ed 2>/dev/null || printf "\033[J"

    echo "Watching Argo workflow processes. Press Ctrl+C to stop."
    echo "Refresh interval: ${WATCH_INTERVAL}s"
    echo

    QUIET_HEADER=true run_once || true

    sleep "$WATCH_INTERVAL"
  done
}

run_ecofloc_watch() {
  cleanup_watch() {
    tput cnorm 2>/dev/null || true
    tput rmcup 2>/dev/null || true
    printf "\nStopped EcoFloc watch.\n"
    exit 0
  }

  trap cleanup_watch INT TERM

  tput smcup 2>/dev/null || true
  tput civis 2>/dev/null || true

  while true; do
    tput cup 0 0 2>/dev/null || printf "\033[H"
    tput ed 2>/dev/null || printf "\033[J"

    echo "Watching EcoFloc measurements for Argo workflow processes. Press Ctrl+C to stop."
    echo "Watch refresh interval: ${WATCH_INTERVAL}s"
    echo "EcoFloc interval: ${ECOFLOC_INTERVAL} ms"
    echo "EcoFloc duration: ${ECOFLOC_DURATION} s"
    echo "EcoFloc metrics: ${ECOFLOC_METRICS}"
    if [ -n "$ECOFLOC_EXPORT_PATH" ]; then
      echo "EcoFloc export path: ${ECOFLOC_EXPORT_PATH}"
    fi
    echo

    run_ecofloc_once || true

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
      if [ $# -lt 2 ]; then
        echo "Missing value for --ecofloc-duration"
        exit 1
      fi

      ECOFLOC_DURATION="$2"
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

    --no-sudo)
      ECOFLOC_USE_SUDO=false
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