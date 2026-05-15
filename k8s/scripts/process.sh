#!/usr/bin/env bash

# Interact with processes running in the Argo workflow

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

PROFILE="domolandes"
COMMAND_WRAPPER=(minikube ssh -p "${PROFILE}" --)
WATCH_INTERVAL="1"

print_help() {
  cat <<'EOF'
Usage: process.sh [options] [command]

Options:
  get                       Get the PID of the processes running in the Argo workflow.
  --full                    Show full command output for matching processes.
  --watch                   Refresh the output continuously.
  --interval SECONDS        Refresh interval for --watch. Default: 1.
  --help                    Show this help

Example:
  process.sh get
  process.sh --full get
  process.sh --watch get
  process.sh --watch --full get
  process.sh --watch --interval 0.5 get
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
            return ts[i + 1]

        if tok.startswith("--function="):
            return tok.split("=", 1)[1]

    m = re.search(r"--function(?:=|\s+)([^ ]+)", cmd)
    if m:
        return m.group(1)

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

print("{:<8} | {:<40} | {:<20}".format("PID", "SCRIPT", "FUNCTION"))
print("{:<8}-+-{:<40}-+-{:<20}".format("--------", "----------------------------------------", "--------------------"))

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
            return ts[i + 1]

        if tok.startswith("--function="):
            return tok.split("=", 1)[1]

    m = re.search(r"--function(?:=|\s+)([^ ]+)", cmd)
    if m:
        return m.group(1)

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

        print("{:<8} | {:<40} | {:<20}".format(pid, script, function))

except KeyboardInterrupt:
    sys.exit(0)
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

run_watch() {
  cleanup_watch() {
    tput cnorm 2>/dev/null || true
    tput rmcup 2>/dev/null || true
    exit 0
  }

  trap cleanup_watch INT TERM

  # Alternate screen + hidden cursor = less flickering.
  tput smcup 2>/dev/null || true
  tput civis 2>/dev/null || true

  while true; do
    # Move cursor to top-left instead of clearing the whole terminal.
    tput cup 0 0 2>/dev/null || printf '\033[H'

    {
      echo "Watching Argo workflow processes. Press Ctrl+C to stop."
      echo "Refresh interval: ${WATCH_INTERVAL}s"
      echo

      QUIET_HEADER=true run_once || true

      # Clear leftover lines from the previous frame.
      tput ed 2>/dev/null || printf '\033[J'
    }

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