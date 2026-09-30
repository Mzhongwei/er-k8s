#!/usr/bin/env bash
# Internal multi-node EcoFLOC collector used by the pipeline lifecycle.

set -euo pipefail

SCAN_INTERVAL="0.1"
ECOFLOC_INTERVAL="1000"
METRICS="cpu,ram,sd,nic,gpu"
RUN_ID=""
RESULT_DIR=""
READY_FILE=""
STOP_FILE=""
# The monitor's node list is deployment-specific *access* config (transport + sudo mode),
# which is not derivable from `kubectl get nodes` -- k3s/VM nodes and their ssh/vm hops do
# not map to Kubernetes node names. So it is read from a config file when present, falling
# back to these built-in defaults. Each line: name=kind:target:mode (kind = local|ssh|vm).
# `--node` on the command line overrides both. See energy-nodes.conf.example.
NODES=(
  "server2-labo=local:execute"
  "zhongwei-lap=ssh:zhongwei-lap:direct"
  "server1-k3s-worker=vm:server1:direct"
)
CUSTOM_NODES=false
PROCESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENERGY_NODES_FILE="${ENERGY_NODES_FILE:-$PROCESS_DIR/energy-nodes.conf}"

# Bound every ssh so an offline/hung node cannot make the monitor (and, via pipeline.sh's
# stop wait, the whole run) block forever. BatchMode avoids interactive password prompts.
SSH_OPTS=(-o ConnectTimeout=5 -o BatchMode=yes -o ServerAliveInterval=5 -o ServerAliveCountMax=2)

# When true, require a Kubernetes Pod UID. STARTED_AFTER also excludes workers that were
# already running when this agent started.
K8S_ONLY=false
STARTED_AFTER=""

parse_node() {
  local spec="$1" name="${1%%=*}" rest="${1#*=}" kind target mode
  [ -n "$name" ] && [ "$rest" != "$spec" ] || return 1
  kind="${rest%%:*}"
  rest="${rest#*:}"
  case "$kind" in
    local) target=""; mode="$rest" ;;
    ssh|vm) target="${rest%:*}"; mode="${rest##*:}" ;;
    *) return 1 ;;
  esac
  case "$mode" in execute|direct) ;; *) return 1 ;; esac
  [ "$kind" = local ] || [ -n "$target" ] || return 1
  printf '%s|%s|%s|%s\n' "$name" "$kind" "$target" "$mode"
}

target_processes() {
  # Emit: pid, process start tick, Pod UID, container ID, task, command.
  ps axww -o pid=,args= | K8S_ONLY="$K8S_ONLY" STARTED_AFTER="$STARTED_AFTER" python3 -c '
import os, re, sys
k8s_only = os.environ.get("K8S_ONLY") == "true"
started_after = float(os.environ.get("STARTED_AFTER") or 0)
pod_re = re.compile(r"pod([0-9a-fA-F]{8}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{12})")
cid_re = re.compile(r"([0-9a-f]{64})")
clock_ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
with open("/proc/stat", encoding="utf-8") as handle:
    boot_time = next(float(line.split()[1]) for line in handle if line.startswith("btime "))

def pod_info(pid):
    try:
        with open(f"/proc/{pid}/cgroup", encoding="utf-8") as handle:
            data = handle.read()
    except OSError:
        return "", ""
    uid = pod_re.search(data)
    cid = cid_re.search(data.strip().split("/")[-1]) or cid_re.search(data)
    return (uid.group(1).replace("_", "-") if uid else ""), (cid.group(1) if cid else "")

def process_start(pid):
    try:
        fields = open(f"/proc/{pid}/stat", encoding="utf-8").read().rsplit(")", 1)[1].split()
        ticks = fields[19]
        return ticks, boot_time + int(ticks) / clock_ticks
    except (OSError, ValueError, IndexError):
        return "", 0

for line in sys.stdin:
    fields = line.strip().split(None, 2)
    if len(fields) < 2:
        continue
    pid = fields[0]
    argv0 = fields[1]
    rest = fields[2] if len(fields) > 2 else ""
    exe = os.path.basename(argv0)
    script = rest.split(None, 1)[0] if rest else ""
    if not ((exe == "python" or exe.startswith("python3")) and script.startswith("/app/")):
        continue
    uid, cid = pod_info(pid)
    if k8s_only and not uid:
        continue
    start_ticks, start_epoch = process_start(pid)
    if not start_ticks or start_epoch < started_after:
        continue
    command = (argv0 + " " + rest).strip()
    task = os.path.basename(script)
    print(f"{pid}\t{start_ticks}\t{uid}\t{cid}\t{task}\t{command}")
'
}

process_alive() {
  local pid="$1" expected="$2" current stat rest
  local -a fields
  [ -r "/proc/$pid/stat" ] || return 1
  stat="$(<"/proc/$pid/stat")"
  rest="${stat##*) }"
  read -r -a fields <<< "$rest"
  current="${fields[19]:-}"
  [ -n "$current" ] && [ "$current" = "$expected" ]
}

descendants() {
  # Print every descendant PID of $1 (children, grandchildren, ...), deepest first.
  local child
  for child in $(pgrep -P "$1" 2>/dev/null || true); do
    descendants "$child"
    printf '%s\n' "$child"
  done
}

stop_tree() {
  # Signal sudo/wrapper/EcoFLOC descendants and allow EcoFLOC to flush its summary.
  local root="$1" p tries pids alive
  pids="$(descendants "$root") $root"
  for p in $pids; do kill -INT "$p" 2>/dev/null || sudo -n kill -INT "$p" 2>/dev/null || true; done
  for tries in 1 2 3 4 5 6 7 8 9 10; do
    alive=false
    for p in $pids; do [ -d "/proc/$p" ] && alive=true; done
    [ "$alive" = true ] || return 0
    sleep 0.2
  done
  pids="$pids $(descendants "$root")"
  for p in $pids; do kill -TERM "$p" 2>/dev/null || sudo -n kill -TERM "$p" 2>/dev/null || true; done
  sleep 0.3
  pids="$pids $(descendants "$root") $root"
  for p in $pids; do kill -KILL "$p" 2>/dev/null || sudo -n kill -KILL "$p" 2>/dev/null || true; done
}

parse_log() {
  local log="$1" avg total status="ok"
  avg="$(awk -F: 'BEGIN{IGNORECASE=1}/Average[[:space:]]+Power/{gsub(/^[ \t]+|[ \t]+$/,"",$2);v=$2}END{print v}' "$log" 2>/dev/null | awk '{print $1}')"
  total="$(awk -F: 'BEGIN{IGNORECASE=1}/Total.*Energy/{gsub(/^[ \t]+|[ \t]+$/,"",$2);v=$2}END{print v}' "$log" 2>/dev/null | awk '{print $1}')"
  if [ -z "$avg" ] || [ -z "$total" ]; then
    avg="0"; total="0"; status="no_data"
  fi
  printf '%s|%s|%s\n' "$avg" "$total" "$status"
}

agent_main() {
  local node="" sudo_mode="" stop_path="" vm=false
  local sessions rows finished log_dir connector_pid="" finalized=false
  local parsed avg total status test_pid
  while [ $# -gt 0 ]; do
    case "$1" in
      --node) node="$2"; shift 2 ;;
      --sudo-mode) sudo_mode="$2"; shift 2 ;;
      --stop-file) stop_path="$2"; shift 2 ;;
      --run-id) RUN_ID="$2"; shift 2 ;;
      --metrics) METRICS="$2"; shift 2 ;;
      --scan-interval) SCAN_INTERVAL="$2"; shift 2 ;;
      --ecofloc-interval) ECOFLOC_INTERVAL="$2"; shift 2 ;;
      --k8s-only) K8S_ONLY=true; shift ;;
      --vm) vm=true; shift ;;
      *) shift ;;
    esac
  done
  STARTED_AFTER="${STARTED_AFTER:-$(date +%s.%N)}"

  sessions="$(mktemp)"
  rows="$(mktemp)"
  finished="$(mktemp)"
  log_dir="/tmp/eaer-ecofloc-${RUN_ID}"
  mkdir -p "$log_dir"
  rm -f "$stop_path"

  ecofloc_cmd=()
  if [ "$sudo_mode" = "execute" ]; then
    ecofloc_cmd=(sudo -n /bin/execute ecofloc)
  else
    ecofloc_cmd=(sudo -n /usr/local/bin/ecofloc)
  fi

  if [ "$vm" = true ] && [ -f "${ERCTL_VM_CONNECTOR_SCRIPT:-/home/vagrant/update_freq.sh}" ]; then
    sh "${ERCTL_VM_CONNECTOR_SCRIPT:-/home/vagrant/update_freq.sh}" >"$log_dir/connector.log" 2>&1 &
    connector_pid="$!"
  fi

  finalize_session() {
    # Keep task and Pod identity with the session rather than joining later by PID.
    local pid="$1" proc_start="$2" metric="$3" eco_pid="$4" log="$5" started="$6" task="$7" pod_uid="$8" container_id="$9"
    local parsed avg total status
    stop_tree "$eco_pid"
    wait "$eco_pid" 2>/dev/null || true
    parsed="$(parse_log "$log")"
    IFS='|' read -r avg total status <<< "$parsed"
    printf '%s\t%s\t%s\n' "$pid" "$proc_start" "$metric" >> "$finished"
    printf 'RESULT\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$node" "$pid" "$proc_start" "$pod_uid" "$container_id" "$metric" "$avg" "$total" \
      "$status" "$started" "$(date +%s)" "$task"
  }

  finalize_all() {
    # The EXIT trap drains sessions on normal exit, signals, and set -e failures.
    [ "$finalized" = true ] && return 0
    finalized=true
    local pid proc_start metric eco_pid log started task pod_uid container_id
    while IFS=$'\t' read -r pid proc_start metric eco_pid log started task pod_uid container_id; do
      [ -n "${pid:-}" ] && finalize_session "$pid" "$proc_start" "$metric" "$eco_pid" "$log" "$started" "$task" "$pod_uid" "$container_id"
    done < "$sessions" 2>/dev/null || true
    : > "$sessions" 2>/dev/null || true
    [ -n "$connector_pid" ] && kill "$connector_pid" 2>/dev/null || true
    rm -f "$sessions" "$rows" "$finished"
  }
  trap finalize_all EXIT
  trap 'exit 0' INT TERM HUP

  # Use real CPU work: a sleeping PID can legitimately report 0 J and therefore cannot
  # prove that this host supports usable EcoFLOC CPU measurements.
  bash -c 'end=$((SECONDS + 10)); while (( SECONDS < end )); do :; done' &
  test_pid="$!"
  if ! "${ecofloc_cmd[@]}" --cpu -p "$test_pid" -i 1000 -t 2 > "$log_dir/preflight.log" 2>&1; then
    kill "$test_pid" 2>/dev/null || true
    wait "$test_pid" 2>/dev/null || true
    echo "EcoFLOC preflight command failed on $node" >&2
    exit 1
  fi
  parsed="$(parse_log "$log_dir/preflight.log")"
  IFS='|' read -r avg total status <<< "$parsed"
  if grep -qiE 'CLOSED OR INEXISTENT|inexistent' "$log_dir/preflight.log" \
      || [ "$status" != "ok" ] \
      || ! awk -v total="$total" 'BEGIN { exit !(total + 0 > 0) }'; then
    kill "$test_pid" 2>/dev/null || true
    wait "$test_pid" 2>/dev/null || true
    echo "EcoFLOC preflight produced no positive CPU energy measurement on $node" >&2
    exit 1
  fi
  kill "$test_pid" 2>/dev/null || true
  wait "$test_pid" 2>/dev/null || true
  printf 'READY\t%s\n' "$node"

  while [ ! -f "$stop_path" ]; do
    while IFS=$'\t' read -r pid proc_start pod_uid container_id task command; do
      [ -n "${pid:-}" ] || continue
      if ! grep -q "^${pid}"$'\t'"${proc_start}"$'\t' "$rows" 2>/dev/null; then
        printf '%s\t%s\t%s\n' "$pid" "$proc_start" "$command" >> "$rows"
        printf 'ROW\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$node" "$pid" "$proc_start" "$pod_uid" "$container_id" "$task" "$command"
      fi
      IFS=',' read -r -a metric_list <<< "$METRICS"
      for metric in "${metric_list[@]}"; do
        metric="${metric// /}"
        grep -q "^${pid}"$'\t'"${proc_start}"$'\t'"${metric}"$'\t' "$sessions" 2>/dev/null && continue
        grep -q "^${pid}"$'\t'"${proc_start}"$'\t'"${metric}"$ "$finished" 2>/dev/null && continue
        log="$log_dir/${node}_${pid}_${metric}.log"
        "${ecofloc_cmd[@]}" "--${metric}" -p "$pid" -i "$ECOFLOC_INTERVAL" -t -1 > "$log" 2>&1 &
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
          "$pid" "$proc_start" "$metric" "$!" "$log" "$(date +%s)" "$task" "$pod_uid" "$container_id" >> "$sessions"
      done
    done < <(target_processes)

    tmp="$(mktemp)"
    while IFS=$'\t' read -r pid proc_start metric eco_pid log started task pod_uid container_id; do
      [ -n "${pid:-}" ] || continue
      if process_alive "$pid" "$proc_start" && [ -d "/proc/$eco_pid" ]; then
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
          "$pid" "$proc_start" "$metric" "$eco_pid" "$log" "$started" "$task" "$pod_uid" "$container_id" >> "$tmp"
      else
        finalize_session "$pid" "$proc_start" "$metric" "$eco_pid" "$log" "$started" "$task" "$pod_uid" "$container_id"
      fi
    done < "$sessions"
    mv "$tmp" "$sessions"
    sleep "$SCAN_INTERVAL"
  done

  exit 0
}

coordinator_main() {
  local fifo remote_script remote_stop ready_count=0 stop_sent=false alive
  local processes_file sessions_file agents_file ready_signalled=false dead_count=0 pending total_nodes
  local -a child_pids=() child_nodes=()
  local -A ready_map=() dead_map=() configured_names=()

  for spec in "${NODES[@]}"; do
    if ! parsed="$(parse_node "$spec")"; then
      echo "Invalid energy node specification: $spec" >&2
      return 1
    fi
    name="${parsed%%|*}"
    if [ -n "${configured_names[$name]:-}" ]; then
      echo "Duplicate energy node name: $name" >&2
      return 1
    fi
    configured_names[$name]=1
  done

  mkdir -p "$RESULT_DIR"
  processes_file="$RESULT_DIR/processes.tsv"
  sessions_file="$RESULT_DIR/sessions.tsv"
  agents_file="$RESULT_DIR/agents.tsv"
  printf 'node\tpid\tprocess_start\tpod_uid\tcontainer_id\ttask\tcommand\n' > "$processes_file"
  printf 'node\tpid\tprocess_start\tpod_uid\tcontainer_id\tmetric\taverage_power_w\ttotal_energy_j\tstatus\tstarted_at\tended_at\ttask\n' > "$sessions_file"
  printf 'node\tstatus\n' > "$agents_file"
  total_nodes="${#NODES[@]}"
  fifo="$(mktemp -u)"
  mkfifo "$fifo"
  exec 3<> "$fifo"
  remote_script="/tmp/eaer-process-${RUN_ID}.sh"
  remote_stop="/tmp/eaer-energy-${RUN_ID}.stop"
  local remote_logdir="/tmp/eaer-ecofloc-${RUN_ID}"
  local k8s_flag=""
  [ "$K8S_ONLY" = true ] && k8s_flag="--k8s-only"

  request_stop() {
    local parsed name kind target mode
    for spec in "${NODES[@]}"; do
      parsed="$(parse_node "$spec")"
      IFS='|' read -r name kind target mode <<< "$parsed"
      case "$kind" in
        local) : > "$remote_stop" ;;
        ssh) ssh "${SSH_OPTS[@]}" "$target" "touch '$remote_stop'" 2>/dev/null || true ;;
        vm) ssh "${SSH_OPTS[@]}" "$target" "ssh -o ConnectTimeout=5 -o BatchMode=yes vm \"touch '$remote_stop'\"" 2>/dev/null || true ;;
      esac
    done
  }
  trap 'if [ "$stop_sent" = false ]; then request_stop 2>/dev/null || true; fi' EXIT
  trap 'stop_sent=true; request_stop 2>/dev/null || true' INT TERM HUP

  for spec in "${NODES[@]}"; do
    parsed="$(parse_node "$spec")"
    IFS='|' read -r name kind target mode <<< "$parsed"
    case "$kind" in
      local)
        bash "$0" __agent__ --node "$name" --sudo-mode "$mode" --run-id "$RUN_ID" \
          --stop-file "$remote_stop" --metrics "$METRICS" --scan-interval "$SCAN_INTERVAL" \
          --ecofloc-interval "$ECOFLOC_INTERVAL" $k8s_flag \
          >&3 2>"$RESULT_DIR/${name}-agent.log" &
        child_pids+=("$!"); child_nodes+=("$name")
        ;;
      ssh)
        if ssh "${SSH_OPTS[@]}" "$target" "cat > '$remote_script' && chmod +x '$remote_script'" < "$0" 2>/dev/null; then
          ssh "${SSH_OPTS[@]}" "$target" "exec bash '$remote_script' __agent__ --node '$name' --sudo-mode '$mode' --run-id '$RUN_ID' --stop-file '$remote_stop' --metrics '$METRICS' --scan-interval '$SCAN_INTERVAL' --ecofloc-interval '$ECOFLOC_INTERVAL' $k8s_flag" \
            >&3 2>"$RESULT_DIR/${name}-agent.log" &
          child_pids+=("$!"); child_nodes+=("$name")
        else
          echo "[warn] cannot reach node $name ($target); skipping it" >&2
          printf '%s\tunavailable\n' "$name" >> "$agents_file"
        fi
        ;;
      vm)
        if ssh "${SSH_OPTS[@]}" "$target" "ssh -o ConnectTimeout=5 -o BatchMode=yes -o ServerAliveInterval=5 -o ServerAliveCountMax=2 vm \"cat > '$remote_script' && chmod +x '$remote_script'\"" < "$0" 2>/dev/null; then
          ssh "${SSH_OPTS[@]}" "$target" "ssh -o ConnectTimeout=5 -o BatchMode=yes -o ServerAliveInterval=5 -o ServerAliveCountMax=2 vm \"exec bash '$remote_script' __agent__ --vm --node '$name' --sudo-mode '$mode' --run-id '$RUN_ID' --stop-file '$remote_stop' --metrics '$METRICS' --scan-interval '$SCAN_INTERVAL' --ecofloc-interval '$ECOFLOC_INTERVAL' $k8s_flag\"" \
            >&3 2>"$RESULT_DIR/${name}-agent.log" &
          child_pids+=("$!"); child_nodes+=("$name")
        else
          echo "[warn] cannot reach VM node $name ($target); skipping it" >&2
          printf '%s\tunavailable\n' "$name" >> "$agents_file"
        fi
        ;;
    esac
  done

  archive_logs() {
    # Copy each node's raw EcoFLOC logs into the permanent results dir so they are not left
    # behind in the nodes' /tmp. Best-effort and bounded by SSH_OPTS.
    local parsed name kind target mode dest
    mkdir -p "$RESULT_DIR/logs"
    for spec in "${NODES[@]}"; do
      parse_node "$spec" > /dev/null 2>&1 || continue
      parsed="$(parse_node "$spec")"
      IFS='|' read -r name kind target mode <<< "$parsed"
      dest="$RESULT_DIR/logs/$name"
      mkdir -p "$dest"
      case "$kind" in
        local) cp -r "$remote_logdir"/. "$dest"/ 2>/dev/null || true ;;
        ssh) scp "${SSH_OPTS[@]}" -r "$target:$remote_logdir/." "$dest"/ 2>/dev/null || true ;;
        vm) ssh "${SSH_OPTS[@]}" "$target" "ssh -o ConnectTimeout=5 -o BatchMode=yes vm \"tar -C '$remote_logdir' -cf - . 2>/dev/null\"" 2>/dev/null | tar -C "$dest" -xf - 2>/dev/null || true ;;
      esac
    done
  }
  while true; do
    got_line=false
    if IFS= read -r -t 0.2 line <&3; then
      got_line=true
      case "$line" in
        READY$'\t'*)
          name="${line#READY$'\t'}"
          if [ -z "${ready_map[$name]:-}" ]; then
            ready_map[$name]=1
            ready_count=$((ready_count + 1))
            printf '%s\tready\n' "$name" >> "$agents_file"
          fi
          ;;
        ROW$'\t'*) printf '%s\n' "${line#*$'\t'}" >> "$processes_file" ;;
        RESULT$'\t'*) printf '%s\n' "${line#*$'\t'}" >> "$sessions_file" ;;
      esac
    fi

    if [ -f "$STOP_FILE" ] && [ "$stop_sent" = false ]; then
      request_stop
      stop_sent=true
    fi

    # Record both startup failures and unexpected failures after READY.
    alive=0
    for idx in "${!child_pids[@]}"; do
      name="${child_nodes[$idx]}"
      if kill -0 "${child_pids[$idx]}" 2>/dev/null; then
        alive=$((alive + 1))
      elif [ -z "${dead_map[$name]:-}" ]; then
        dead_map[$name]=1
        dead_count=$((dead_count + 1))
        if [ -n "${ready_map[$name]:-}" ]; then
          unset 'ready_map[$name]'
          ready_count=$((ready_count - 1))
          if [ "$stop_sent" = true ]; then
            printf '%s\tcompleted\n' "$name" >> "$agents_file"
          else
            printf '%s\tfailed_after_ready\n' "$name" >> "$agents_file"
            echo "[warn] EcoFLOC agent for $name stopped during measurement" >&2
          fi
          [ "$ready_signalled" = true ] && printf '%s\n' "$ready_count" > "$READY_FILE"
        else
          printf '%s\tunavailable\n' "$name" >> "$agents_file"
          echo "[warn] EcoFLOC agent for $name exited before READY" >&2
        fi
      fi
    done

    # Strict preflight: every configured node must be Ready before workload creation. An
    # unreachable or failed node produces 0 even if some other agents passed.
    if [ "$ready_signalled" = false ]; then
      pending=$(( ${#child_pids[@]} - ready_count - dead_count ))
      if [ "$pending" -le 0 ]; then
        if [ "$ready_count" -eq "$total_nodes" ]; then
          printf '%s\n' "$ready_count" > "$READY_FILE"
        else
          printf '0\n' > "$READY_FILE"
        fi
        ready_signalled=true
      fi
    fi

    # Stay alive until the pipeline requests stop, even when no agent survived startup.
    [ "$stop_sent" = true ] && [ "$alive" -eq 0 ] && [ "$got_line" = false ] && break
  done

  for pid in "${child_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  archive_logs
  trap - EXIT INT TERM HUP
  rm -f "$fifo" "$READY_FILE" "$STOP_FILE"
}

if [ "${1:-}" = "__agent__" ]; then
  shift
  agent_main "$@"
  exit 0
fi

while [ $# -gt 0 ]; do
  case "$1" in
    --run-id) RUN_ID="$2"; shift 2 ;;
    --result-dir) RESULT_DIR="$2"; shift 2 ;;
    --ready-file) READY_FILE="$2"; shift 2 ;;
    --stop-file) STOP_FILE="$2"; shift 2 ;;
    --metrics) METRICS="$2"; shift 2 ;;
    --k8s-only) K8S_ONLY=true; shift ;;
    --node)
      if [ "$CUSTOM_NODES" = false ]; then NODES=(); CUSTOM_NODES=true; fi
      NODES+=("$2"); shift 2
      ;;
    *) echo "Unknown process option: $1" >&2; exit 1 ;;
  esac
done

[ -n "$RUN_ID" ] && [ -n "$RESULT_DIR" ] && [ -n "$READY_FILE" ] && [ -n "$STOP_FILE" ] || {
  echo "process.sh is internal; use 'erctl pipeline start --energy-monitor'." >&2
  exit 1
}

# Load the node list from the config file unless --node was used. Blank lines and #comments
# are ignored; each remaining line is one name=kind:target:mode spec.
if [ "$CUSTOM_NODES" = false ] && [ -f "$ENERGY_NODES_FILE" ]; then
  NODES=()
  while IFS= read -r spec || [ -n "$spec" ]; do
    spec="${spec%%#*}"
    spec="$(printf '%s' "$spec" | xargs)"
    [ -n "$spec" ] && NODES+=("$spec")
  done < "$ENERGY_NODES_FILE"
  echo "[info] loaded ${#NODES[@]} node(s) from $ENERGY_NODES_FILE" >&2
fi

coordinator_main
