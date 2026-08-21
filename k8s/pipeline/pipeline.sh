#!/usr/bin/env bash

# Manage the Kubernetes/Argo pipeline lifecycle.
# This script is normally called through:
#   k8s/erctl.sh pipeline ...
#
# Lifecycle overview:
#   start     Full setup + run. Compiles manifests, wipes prior Jobs/Workflows/PVCs for
#             this namespace, recreates PVCs, lets you edit the config file passed via
#             -c/--config, then derives the ConfigMap contents and batch vs incremental
#             dispatch from that file's `mode:` field:
#               - embedding-training-inference-evaluation: runs the batch training Argo
#                 Workflow to completion first (seeds graph/embedding/index models), then
#                 applies the incremental worker Jobs (producer/consumer/normalization/.../
#                 evaluation) as plain `kind: Job` manifests -- NOT part of any Argo
#                 Workflow. These run concurrently and stream until the input exhausts.
#               - bert-training-evaluation: runs entirely as one Argo Workflow (no
#                 incremental phase).
#   stop      Non-destructive cancellation: stops the newest `pipeline-*` Argo Workflow
#             and deletes incremental worker Jobs, while preserving ConfigMaps, runtime
#             PVCs, and saved results. Stopped work cannot be resumed in place.
#   terminate Force-stops the Argo Workflow (if any), then hard-resets: deletes the EAER
#             ConfigMaps and wipes+recreates the pipeline's PVCs -- this also clears out
#             the incremental worker Jobs, since deleting $PIPELINE_INCREMENTAL_DIR is part
#             of that PVC/Job cleanup. See terminate_pipeline() for the exact scope.

# If this script is launched by sh or another non-Bash shell, re-exec it with bash.
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

# get the right work directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
K8S_DIR="$ROOT_DIR/k8s"

# Generated Argo batch workflow manifest used for batch/training modes.
PIPELINE_BATCH_PATH="$K8S_DIR/pipeline/exec/batch/pipeline.yaml"

# Generated Kubernetes Job manifests used for incremental mode.
PIPELINE_INCREMENTAL_DIR="$K8S_DIR/pipeline/exec/incremental"
PIPELINE_INCREMENTAL_WORKERS_DIR="$PIPELINE_INCREMENTAL_DIR/workers"

# Directory containing PVC manifests that must exist before jobs/workflows run.
PVC_MANIFESTS="$K8S_DIR/pvc-manifests"

# Long-lived read-only dataset volume. It is outside PVC_MANIFESTS so pipeline cleanup
# never deletes it.
DATASET_VOLUME_MANIFEST="$K8S_DIR/datasets/dataset-volume.yaml"

# Kubernetes namespace used by Argo and the pipeline resources. All `kubectl`/`argo` calls
# below are scoped to this namespace, including PVC create/delete -- PersistentVolumeClaims
# are namespaced, so `kubectl delete pvc -n "$NAMESPACE" --all` only ever touches this
# namespace's claims, never another namespace's. Note this value must match the hardcoded
# `namespace: argo` in every k8s/pvc-manifests/*.yaml: `kubectl apply -n X -f file.yaml`
# errors out if X differs from the namespace already set in the manifest, so changing this
# variable alone is not enough to move the pipeline to a different namespace.
NAMESPACE="argo"

# Path to the pipeline config YAML, set via -c/--config. Required -- the file's own
# `mode:` field is the sole source of truth for the ConfigMap contents and batch vs
# incremental dispatch below.
CONFIG_PATH=""

# The pipeline intentionally supports exactly these two end-to-end business modes.
EMBEDDING_PIPELINE_MODE="embedding-training-inference-evaluation"
BERT_PIPELINE_MODE="bert-training-evaluation"

# Energy monitoring backends. EcoFLOC is started per run; Alumet is a cluster service and
# this script exports the current run's time window from its InfluxDB backend.
PROCESS_SCRIPT="$K8S_DIR/monitoring/ecofloc/process.sh"
ALUMET_SCRIPT="$K8S_DIR/monitoring/alumet/alumet.py"
RESULTS_SCRIPT="$K8S_DIR/monitoring/results.py"
SCHEDULING_COMPILER="$K8S_DIR/scheduling/compiler.py"
SCHEDULING_PLAN_PATH="$K8S_DIR/pipeline/exec/scheduling-plan.tsv"

# Root under which each run's permanent results are stored, one directory per run:
#   k8s/results/<mode>-<timestamp>/
#     energy/ecofloc-summary.json normalized EcoFLOC totals (when selected)
#     energy/alumet-summary.json  normalized Alumet totals (when selected)
#     energy/sessions.tsv  EcoFLOC session records (EcoFLOC backend only)
#     energy/alumet-raw.csv raw InfluxDB export (Alumet backend only)
#     monitor.log          EcoFLOC coordinator diagnostics (EcoFLOC backend only)
#     matching-result.txt  the pipeline's own [Result]/[RESULT] line
#     scheduling-plan.tsv  strategy candidates, normalized scores, roles, and reasons
#     placement.tsv        task Pod -> Kubernetes node mapping and timestamps
RESULTS_DIR="$K8S_DIR/results"

# --energy-monitor: use EcoFLOC (default), Alumet, or both, then permanently save each
#   provider's independent report under RESULTS_DIR. Off by default.
ENERGY_MONITOR=false
ENERGY_MONITOR_TOOL="ecofloc"
ECOFLOC_MONITOR_ACTIVE=false
ALUMET_MONITOR_ACTIVE=false
# --results-summary: after the workload finishes, print the matching result (and, if energy
#   was monitored, the energy summary). Independent of --energy-monitor. Off by default.
RESULTS_SUMMARY=false
PLAN_ONLY=false

# --results-archive DEST (or env ERCTL_RESULTS_ARCHIVE): after the run, copy this run's
# results dir to a durable/remote location -- a local/mounted path, or user@host:/path
# (rsync/scp). The control-node k8s/results/ dir alone is gitignored and not a backup.
RESULTS_ARCHIVE="${ERCTL_RESULTS_ARCHIVE:-}"

# Populated at runtime by start_run / record_matching_result.
RUN_DIR=""          # this run's permanent results directory
MONITOR_PID=""      # PID of the background process.sh coordinator, if started
MONITOR_READY_FILE=""
MONITOR_STOP_FILE=""
MATCHING_RESULT=""  # the captured [Result]/[RESULT] line for this run
CURRENT_MODE=""
PIPELINE_STATUS="Running"
ACTIVE_WORKFLOW=""
PIPELINE_STOPPING=false

usage() {
    # Print command help without executing any cluster operation.
    cat << 'EOF'
Usage: erctl pipeline [start|stop|terminate] [options]
Manage the pipeline.
Actions:
    start                    Start the pipeline (default)
    stop                     Cancel active workloads but preserve PVCs and ConfigMaps
    terminate                Terminate the latest pipeline and clean up resources
Options (start):
    -c, --config PATH        Path to the pipeline config YAML (required); its `mode:` field
                             decides the ConfigMaps and batch vs incremental dispatch
    --energy-monitor [TOOL]  Save energy for the whole workload. TOOL is ecofloc (default),
                             alumet, or ecofloc-alumet. Also accepts --energy-monitor=TOOL.
    --results-summary        After the workload finishes, print matching, Pod placement,
                             and energy (when monitored).
    --plan-only              Show and save the scheduling plan without creating workloads
                             or changing PVCs, ConfigMaps, Workflows, or Jobs.
    --results-archive DEST   Copy this run's results dir to a durable/remote location after
                             the run (local/mounted path, or user@host:/path). Also settable
                             via ERCTL_RESULTS_ARCHIVE.
    -h, --help, -help, help  Show this help
EOF
}

require_cmd() {
    # Ensure a required command is available before starting a workflow action.
    local cmd="$1"

    # `command -v` returns non-zero when the executable is not in PATH.
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Required command not found: $cmd"
        exit 1
    fi
}

latest_pipeline_workflow() {
    # Find the most recently created Argo Workflow whose name starts with pipeline-.
    # Steps:
    # 1. List workflow names with creation timestamps.
    # 2. Keep only pipeline-* workflows.
    # 3. Sort by timestamp.
    # 4. Return the newest workflow name.
    kubectl get wf -n "$NAMESPACE" --no-headers -o custom-columns=NAME:.metadata.name,CREATED:.metadata.creationTimestamp \
        | awk '$1 ~ /^pipeline-/ { print $1 "\t" $2 }' \
        | sort -k2,2 \
        | tail -n 1 \
        | cut -f1
}

wait_for_workflow_completion() {
    local workflow_name="$1"
    local phase
    while true; do
        if ! phase="$(kubectl get wf -n "$NAMESPACE" "$workflow_name" -o jsonpath='{.status.phase}' 2>/dev/null)"; then
            echo "Workflow $workflow_name was deleted; stopping local execution." >&2
            return 1
        fi
        case "$phase" in
            Succeeded|Failed|Error)
                printf '%s\n' "$phase"
                return 0
                ;;
        esac
        sleep 2
    done
}

wait_for_incremental_jobs() {
    # Incremental processing is stream-driven: success is reached only after EOS has
    # propagated through every worker. There is deliberately no elapsed-time deadline
    # here; a slow mini-batch must not be mistaken for an ended stream. Kubernetes Job
    # failure remains the terminal error signal.
    local all_complete
    local complete_status
    local failed_status
    local job_name
    local job_names=(
        calculating-similarity
        candidate-enumeration
        cg-feature-extraction
        decision-making
        embedding-training
        evaluation
        graph-construction
        kafka-consumer
        kafka-producer
        normalization
        random-walk
    )

    while true; do
        all_complete=true

        for job_name in "${job_names[@]}"; do
            if ! kubectl get job -n "$NAMESPACE" "$job_name" >/dev/null 2>&1; then
                echo "Incremental worker job/$job_name was deleted; stopping local execution." >&2
                return 1
            fi
            failed_status="$(
                kubectl get job -n "$NAMESPACE" "$job_name" \
                    -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' \
                    2>/dev/null || true
            )"
            if [ "$failed_status" = "True" ]; then
                echo "Incremental worker job/$job_name failed."
                kubectl logs -n "$NAMESPACE" "job/$job_name" --tail=50 || true
                return 1
            fi

            complete_status="$(
                kubectl get job -n "$NAMESPACE" "$job_name" \
                    -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' \
                    2>/dev/null || true
            )"
            if [ "$complete_status" != "True" ]; then
                all_complete=false
            fi
        done

        if [ "$all_complete" = true ]; then
            return 0
        fi
        sleep 5
    done
}

extract_mode_from_config() {
    # Read the top-level `mode:` field from a YAML-like config file.
    local config_path="$1"

    # This is a simple text extraction, not a full YAML parser.
    awk -F': *' '
        # Match a line such as: mode: embedding-training
        /^[[:space:]]*mode[[:space:]]*:/ {
            # The second field is the value after the colon.
            mode = $2
            # Remove a simple pair of surrounding double quotes if present.
            gsub(/^"|"$/, "", mode)
            # Print the mode and stop after the first match.
            print mode
            exit
        }
    ' "$config_path"
}

delete_pipeline_configmaps() {
    # Delete the EAER script ConfigMaps and the pipeline config ConfigMap, so the next
    # `start` (or a `terminate` cleanup) doesn't leave stale mounted code/config behind.
    kubectl get cm -n "$NAMESPACE" -o name | grep '^configmap/eaer-' | xargs -r kubectl delete -n "$NAMESPACE" || true
    kubectl delete cm -n "$NAMESPACE" er-pipeline-config --ignore-not-found=true
}

delete_pipeline_storage() {
    # Delete pipeline pods, incremental Jobs, Argo workflows and pipeline-owned PVCs.
    # PersistentVolumes are cluster-scoped (no namespace), so they are deliberately NOT
    # deleted here -- `kubectl delete pv --all` would remove every PV in the cluster,
    # including ones bound to unrelated namespaces. PVC selection is likewise derived
    # from this pipeline's manifests instead of using `pvc --all`: the argo namespace may
    # contain long-lived claims owned by other services. Trying to delete such a claim can
    # wait forever on pvc-protection and risks deleting state that this pipeline does not
    # own. Deleting the owned PVCs below is enough:
    # dynamically-provisioned PVs follow the storage class's reclaim policy (nfs-client
    # defaults to Delete), so they get cleaned up as a consequence of their PVC going away,
    # scoped correctly to just this namespace's claims.
    #
    # recreate=true also re-applies the PVC manifests afterward, leaving empty PVCs ready
    # for immediate reuse (start_pipeline's own cleanup pass wants this); recreate=false
    # (the default, used by terminate_pipeline) just tears down and stops there.
    local recreate="${1:-false}"
    local pvc_manifest
    local pvc_name
    local pvc_ref
    local pipeline_pvcs=()

    # Use the manifest directory as the single source of truth for owned claim names.
    while IFS= read -r pvc_manifest; do
        pvc_name="$(
            awk '/^[[:space:]]*name:[[:space:]]*/ { print $2; exit }' "$pvc_manifest"
        )"
        if [ -z "$pvc_name" ]; then
            echo "Unable to read PVC name from manifest: $pvc_manifest"
            return 1
        fi
        pipeline_pvcs+=("pvc/$pvc_name")
    done < <(find "$PVC_MANIFESTS" -maxdepth 1 -type f -name '*.yaml' -print | sort)

    # `timeout 5` keeps deletion of disposable Pods/Workflows best-effort; `xargs -r`
    # skips running kubectl when nothing matched.
    #
    # PVC deletion is intentionally different: it must finish before manifests with the
    # same claim names are applied again. Previously the client was killed after five
    # seconds and the apply raced with deletion still running in the API server. The old
    # claims could then disappear after `kubectl apply` reported success, leaving new
    # workload Pods Pending with "persistentvolumeclaim ... not found".
    timeout 5 kubectl get po -n "$NAMESPACE" -o name | grep '^pod/pipeline-' | xargs -r kubectl delete -n "$NAMESPACE" || true
    kubectl delete -n "$NAMESPACE" -R -f "$PIPELINE_INCREMENTAL_DIR" --ignore-not-found=true || true
    timeout 5 kubectl get wf -n "$NAMESPACE" --no-headers -o custom-columns=NAME:.metadata.name | grep '^pipeline-' | xargs -r argo delete -n "$NAMESPACE" || true
    if [ "${#pipeline_pvcs[@]}" -gt 0 ]; then
        kubectl delete -n "$NAMESPACE" "${pipeline_pvcs[@]}" \
            --ignore-not-found=true \
            --wait=true \
            --timeout=5m
    fi

    if [ "$recreate" = true ]; then
        kubectl apply -n "$NAMESPACE" -f "$PVC_MANIFESTS"
        for pvc_ref in "${pipeline_pvcs[@]}"; do
            kubectl wait -n "$NAMESPACE" \
                --for=jsonpath='{.status.phase}'=Bound \
                "$pvc_ref" \
                --timeout=5m
        done
    fi
}

reap_monitor() {
    # Bound shutdown so an unreachable node cannot block the pipeline forever.
    local pid="$1" waited=0
    [ -n "$pid" ] || return 0
    [ -n "${MONITOR_STOP_FILE:-}" ] && { : > "$MONITOR_STOP_FILE" 2>/dev/null || true; }
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$waited" -ge 150 ]; then   # 150 x 0.2s = 30s
            echo "Warning: energy monitor did not stop within 30s; terminating it." >&2
            kill -TERM "$pid" 2>/dev/null || true
            sleep 1
            kill -KILL "$pid" 2>/dev/null || true
            break
        fi
        sleep 0.2
        waited=$((waited + 1))
    done
    wait "$pid" 2>/dev/null || true
}

RESULTS_ARCHIVED=false
archive_results() {
    # Copy the final run directory once when the script exits.
    [ "$RESULTS_ARCHIVED" = false ] || return 0
    [ -n "${RESULTS_ARCHIVE:-}" ] && [ -n "${RUN_DIR:-}" ] && [ -d "$RUN_DIR" ] || return 0
    RESULTS_ARCHIVED=true
    echo "Archiving results to $RESULTS_ARCHIVE ..."
    case "$RESULTS_ARCHIVE" in
        *:*)
            if command -v rsync >/dev/null 2>&1; then
                rsync -a -e "ssh -o ConnectTimeout=5 -o BatchMode=yes" "$RUN_DIR" "$RESULTS_ARCHIVE/"
            else
                scp -q -o ConnectTimeout=5 -o BatchMode=yes -r "$RUN_DIR" "$RESULTS_ARCHIVE/"
            fi
            ;;
        *)
            mkdir -p "$RESULTS_ARCHIVE"
            cp -r "$RUN_DIR" "$RESULTS_ARCHIVE/"
            ;;
    esac
}

stop_energy_monitor_on_exit() {
    # Preserve partial measurements when the pipeline fails or is interrupted.
    if [ "$ECOFLOC_MONITOR_ACTIVE" = true ] || [ "$ALUMET_MONITOR_ACTIVE" = true ]; then
        stop_energy_monitor >/dev/null 2>&1 || true
    fi
    if [ -n "$RUN_DIR" ] && [ "$PIPELINE_STATUS" != "Succeeded" ]; then
        python3 "$RESULTS_SCRIPT" manifest "$RUN_DIR" \
            --status Failed --mode "$CURRENT_MODE" 2>/dev/null || true
    fi
    archive_results
}

start_run() {
    local mode="$1" ts
    ts="$(date +%Y%m%d-%H%M%S)"
    CURRENT_MODE="$mode"
    RUN_DIR="$RESULTS_DIR/${mode}-${ts}-$$"
    mkdir -p "$RUN_DIR"
    python3 "$RESULTS_SCRIPT" manifest "$RUN_DIR" --status Running --mode "$mode"
    trap stop_energy_monitor_on_exit EXIT
}

show_ecofloc_preflight_diagnostics() {
    [ -f "$RUN_DIR/energy/agents.tsv" ] && cat "$RUN_DIR/energy/agents.tsv" >&2
    local log
    for log in "$RUN_DIR"/energy/*-agent.log "$RUN_DIR"/energy/logs/*/preflight.log; do
        [ -s "$log" ] || continue
        echo "--- ${log#"$RUN_DIR"/} ---" >&2
        cat "$log" >&2
    done
}

start_energy_monitor() {
    # Every requested backend must be ready before workload Pods are created. EcoFLOC is
    # stricter than Alumet: every node listed in energy-nodes.conf must pass preflight.
    [ "$ENERGY_MONITOR" = true ] || return 0

    if [ "$ENERGY_MONITOR_TOOL" = "alumet" ] || [ "$ENERGY_MONITOR_TOOL" = "ecofloc-alumet" ]; then
        if ! python3 "$ALUMET_SCRIPT" start "$RUN_DIR"; then
            echo "Alumet was requested but is not ready; pipeline will not start." >&2
            return 1
        fi
        ALUMET_MONITOR_ACTIVE=true
    fi

    [ "$ENERGY_MONITOR_TOOL" = "ecofloc" ] || [ "$ENERGY_MONITOR_TOOL" = "ecofloc-alumet" ] || return 0
    local run_id
    run_id="$(basename "$RUN_DIR")"
    MONITOR_READY_FILE="$RUN_DIR/.energy-ready"
    MONITOR_STOP_FILE="$RUN_DIR/.energy-stop"

    # Only measure new Kubernetes worker processes; old Pods and host processes are ignored.
    bash "$PROCESS_SCRIPT" \
        --run-id "$run_id" \
        --result-dir "$RUN_DIR/energy" \
        --ready-file "$MONITOR_READY_FILE" \
        --stop-file "$MONITOR_STOP_FILE" \
        --k8s-only \
        > "$RUN_DIR/monitor.log" 2>&1 &
    MONITOR_PID=$!
    ECOFLOC_MONITOR_ACTIVE=true

    local ticks=0
    local ready_timeout_ticks=150   # 150 x 0.2s = 30s
    while [ ! -f "$MONITOR_READY_FILE" ]; do
        if ! kill -0 "$MONITOR_PID" 2>/dev/null; then
            echo "EcoFLOC exited before becoming ready; pipeline will not start." >&2
            wait "$MONITOR_PID" 2>/dev/null || true
            MONITOR_PID=""
            ECOFLOC_MONITOR_ACTIVE=false
            return 1
        fi
        if [ "$ticks" -ge "$ready_timeout_ticks" ]; then
            echo "EcoFLOC was not ready after 30s; pipeline will not start." >&2
            reap_monitor "$MONITOR_PID"
            MONITOR_PID=""
            ECOFLOC_MONITOR_ACTIVE=false
            show_ecofloc_preflight_diagnostics
            return 1
        fi
        sleep 0.2
        ticks=$((ticks + 1))
    done

    local ready_nodes=""
    local active_nodes=""
    [ -f "$MONITOR_READY_FILE" ] && ready_nodes="$(tr -dc '0-9' < "$MONITOR_READY_FILE" 2>/dev/null)"
    if [ -n "$ready_nodes" ] && [ "$ready_nodes" -gt 0 ] 2>/dev/null; then
        active_nodes="$(awk -F '\t' 'NR > 1 && $2 == "ready" { names = names (names ? ", " : "") $1 } END { print names }' "$RUN_DIR/energy/agents.tsv")"
        echo "EcoFLOC ready (measuring $ready_nodes node(s)): $active_nodes"
        echo "Results dir: $RUN_DIR"
    else
        echo "EcoFLOC preflight did not pass on every configured node; pipeline will not start." >&2
        reap_monitor "$MONITOR_PID"
        MONITOR_PID=""
        ECOFLOC_MONITOR_ACTIVE=false
        show_ecofloc_preflight_diagnostics
        return 1
    fi
}

stop_energy_monitor() {
    # Close and summarize each requested backend independently.
    if [ "$ALUMET_MONITOR_ACTIVE" = true ]; then
        ALUMET_MONITOR_ACTIVE=false
        if python3 "$ALUMET_SCRIPT" stop "$RUN_DIR"; then
            echo "Alumet energy summary saved: $RUN_DIR/energy/alumet-summary.json"
        else
            echo "Warning: Alumet returned no usable energy data; workload results remain valid." >&2
        fi
    fi

    if [ "$ECOFLOC_MONITOR_ACTIVE" = true ]; then
        ECOFLOC_MONITOR_ACTIVE=false
        if [ -n "${MONITOR_PID:-}" ]; then
            reap_monitor "$MONITOR_PID"
            MONITOR_PID=""
            if python3 "$RESULTS_SCRIPT" energy "$RUN_DIR"; then
                echo "EcoFLOC energy summary saved: $RUN_DIR/energy/ecofloc-summary.json"
            else
                echo "Warning: EcoFLOC returned no usable energy data; workload results remain valid." >&2
            fi
        fi
    fi
}

record_matching_result() {
    # Store the result log line next to the exported matching files.
    MATCHING_RESULT="$1"
    if [ -n "$RUN_DIR" ]; then
        printf '%s\n' "$MATCHING_RESULT" > "$RUN_DIR/matching-result.txt"
    fi
}

record_placement() {
    local phase="$1"
    local workflow="${2:-}"
    local args=(placement "$RUN_DIR" --namespace "$NAMESPACE" --phase "$phase")
    [ -n "$workflow" ] && args+=(--workflow "$workflow")
    python3 "$RESULTS_SCRIPT" "${args[@]}" \
        || echo "Warning: $phase Pod placement could not be recorded." >&2
}

emit_results_summary() {
    # --results-summary: print the matching result and, if energy was monitored, the energy
    # summary too. Independent of --energy-monitor; no-op unless --results-summary was given.
    [ "$RESULTS_SUMMARY" = true ] || return 0
    if [ -n "$RUN_DIR" ]; then
        python3 "$RESULTS_SCRIPT" show --root "$RESULTS_DIR" --run "$(basename "$RUN_DIR")"
    elif [ -n "$MATCHING_RESULT" ]; then
        echo "$MATCHING_RESULT"
    fi
}

interrupt_pipeline() {
    local exit_code="$1"
    trap - INT TERM
    if [ "$PIPELINE_STOPPING" = true ]; then
        exit "$exit_code"
    fi
    PIPELINE_STOPPING=true
    echo >&2
    echo "Pipeline interrupted; stopping Kubernetes workloads..." >&2
    if [ -n "$RUN_DIR" ]; then
        [ -n "$ACTIVE_WORKFLOW" ] && record_placement batch "$ACTIVE_WORKFLOW"
        record_placement incremental
    fi
    if [ -n "$ACTIVE_WORKFLOW" ]; then
        argo terminate -n "$NAMESPACE" "$ACTIVE_WORKFLOW" >/dev/null 2>&1 || true
    fi
    if [ -d "$PIPELINE_INCREMENTAL_DIR" ]; then
        kubectl delete -n "$NAMESPACE" -R -f "$PIPELINE_INCREMENTAL_DIR" \
            --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
    fi
    exit "$exit_code"
}

ensure_dataset_volume() {
    kubectl apply -f "$DATASET_VOLUME_MANIFEST"
    kubectl wait -n "$NAMESPACE" \
        --for=jsonpath='{.status.phase}'=Bound \
        pvc/pipeline-data-claim \
        --timeout=5m
}

start_pipeline() {
    # Start either the batch Argo workflow or the incremental Kubernetes Jobs.
    local config_mode
    local compiler_mode="all"
    local compiler_args
    local workflow_status

    # Measure total script wall-clock time.
    script_start_time=$(date +%s)

    config_mode="$(extract_mode_from_config "$CONFIG_PATH")"
    if [ -z "$config_mode" ]; then
        echo "Unable to read mode from config file: $CONFIG_PATH"
        exit 1
    fi
    case "$config_mode" in
        "$EMBEDDING_PIPELINE_MODE") compiler_mode="all" ;;
        "$BERT_PIPELINE_MODE") compiler_mode="batch" ;;
        *)
            echo "Unsupported mode in $CONFIG_PATH: $config_mode"
            echo "Supported modes: $EMBEDDING_PIPELINE_MODE, $BERT_PIPELINE_MODE"
            exit 1
            ;;
    esac

    # Resolve and display the plan before any cluster mutation. The same plan is used to
    # generate manifests and, for a real run, copied into the permanent result directory.
    compiler_args=(
        --mode "$compiler_mode"
        --pipeline-mode "$config_mode"
        --plan-output "$SCHEDULING_PLAN_PATH"
        --print-plan
    )
    [ "$PLAN_ONLY" = true ] && compiler_args+=(--plan-only)
    python3 "$SCHEDULING_COMPILER" "${compiler_args[@]}"
    if [ "$PLAN_ONLY" = true ]; then
        echo "Plan-only complete; no Kubernetes workload or storage resource was changed."
        return 0
    fi

    # Wipe pods/Jobs/workflows/PVCs left over from a previous run, then recreate empty
    # PVCs ready for this one. See delete_pipeline_storage() for exactly what this deletes.
    delete_pipeline_storage true

    # Create the long-lived dataset PV/PVC if needed. Re-applying is idempotent and does
    # not copy or clear any dataset files.
    ensure_dataset_volume

    # Delete old EAER script ConfigMaps to avoid stale mounted code, and the previous
    # pipeline config ConfigMap.
    delete_pipeline_configmaps

    # ConfigMaps -- bundles the exact file at $CONFIG_PATH as er-pipeline-config, so the
    # config the workers see at runtime is exactly what was just edited above.
    bash "$SCRIPT_DIR/configmaps.sh" "$CONFIG_PATH"

    # Measure only the actual workload execution time from this point.
    pipeline_start_time=$(date +%s)

    # Every run gets a result directory, even when energy monitoring is disabled.
    start_run "$config_mode"
    cp "$SCHEDULING_PLAN_PATH" "$RUN_DIR/scheduling-plan.tsv"

    # Start the background energy monitor (no-op unless --energy-monitor) so it covers the
    # whole workload -- both the batch Argo phase and the incremental worker phase.
    start_energy_monitor "$config_mode"

    case "$config_mode" in
        "$EMBEDDING_PIPELINE_MODE")
            # Build the graph, embedding model and feature index in Argo. Submit and
            # waiting are split so the workflow name is known and external deletion can
            # terminate this local process instead of leaving it waiting indefinitely.
            ACTIVE_WORKFLOW="$(argo submit -n "$NAMESPACE" "$PIPELINE_BATCH_PATH" -p mode="$config_mode" -o name)"
            if ! workflow_status="$(wait_for_workflow_completion "$ACTIVE_WORKFLOW")"; then
                record_placement batch "$ACTIVE_WORKFLOW"
                exit 1
            fi
            record_placement batch "$ACTIVE_WORKFLOW"
            if [ "$workflow_status" != "Succeeded" ]; then
                echo "Batch training workflow $ACTIVE_WORKFLOW finished with status: $workflow_status (expected Succeeded)."
                echo "Not starting incremental workers -- their inputs would be incomplete."
                exit 1
            fi
            ACTIVE_WORKFLOW=""

            # Start producer, consumer and all processing workers together, including
            # evaluation. decision_making.py writes a per-window predicted-matching
            # snapshot and references it in its buffer event, so evaluation.py -- running
            # concurrently -- evaluates the exact graph produced for that window instead of
            # whatever a shared path happens to hold, matching the business design (evaluate
            # after every decision, report overwritten so only the latest is kept). It exits
            # on its own once it sees decision-making's EOS.
            for worker_manifest in "$PIPELINE_INCREMENTAL_WORKERS_DIR"/*.yaml; do
                kubectl apply -n "$NAMESPACE" -f "$worker_manifest"
            done

            echo "Waiting for incremental worker jobs to complete..."
            if ! wait_for_incremental_jobs; then
                record_placement incremental
                exit 1
            fi
            record_placement incremental

            # Capture the matching result. `|| true` guards the whole substitution: under
            # `set -o pipefail`, a grep that finds no [Result] line would otherwise abort
            # an otherwise-successful run.
            record_matching_result "$(
                kubectl logs -n "$NAMESPACE" -l app=evaluation --tail=-1 2>/dev/null \
                    | grep -F '[Result]' | tail -n 1 || true
            )"
            [ -n "$MATCHING_RESULT" ] && echo "$MATCHING_RESULT"

            ;;

        "$BERT_PIPELINE_MODE")
            # The Argo main DAG routes every bert mode to the fixed
            # bert-training-evaluation-dag sequence.
            ACTIVE_WORKFLOW="$(argo submit -n "$NAMESPACE" "$PIPELINE_BATCH_PATH" -p mode="$config_mode" -o name)"
            if ! workflow_status="$(wait_for_workflow_completion "$ACTIVE_WORKFLOW")"; then
                record_placement batch "$ACTIVE_WORKFLOW"
                exit 1
            fi
            record_placement batch "$ACTIVE_WORKFLOW"
            if [ "$workflow_status" != "Succeeded" ]; then
                echo "BERT workflow $ACTIVE_WORKFLOW finished with status: $workflow_status (expected Succeeded)."
                exit 1
            fi
            ACTIVE_WORKFLOW=""

            # Capture the matching result (see the embedding branch for why `|| true`).
            record_matching_result "$(
                argo logs -n "$NAMESPACE" @latest 2>/dev/null \
                    | grep -F '[RESULT]' | tail -n 1 || true
            )"
            [ -n "$MATCHING_RESULT" ] && echo "$MATCHING_RESULT"

            ;;
    esac

    # Persist workload artifacts before finalizing optional monitoring.
    if [ -n "$RUN_DIR" ]; then
        python3 "$RESULTS_SCRIPT" collect "$RUN_DIR" --namespace "$NAMESPACE" 2>/dev/null \
            || echo "Warning: matching-result artifacts could not be collected." >&2
    fi

    # Energy measurement is auxiliary: each backend stores its own failed/partial status.
    stop_energy_monitor

    if [ -n "$RUN_DIR" ]; then
        python3 "$RESULTS_SCRIPT" manifest "$RUN_DIR" --status Succeeded --mode "$config_mode"
        PIPELINE_STATUS="Succeeded"
        if [ -n "$RESULTS_ARCHIVE" ]; then
            if archive_results; then
                python3 "$RESULTS_SCRIPT" manifest "$RUN_DIR" --status Succeeded \
                    --mode "$config_mode" --archive-status succeeded
            else
                python3 "$RESULTS_SCRIPT" manifest "$RUN_DIR" --status Succeeded \
                    --mode "$config_mode" --archive-status failed
                echo "Warning: workload succeeded, but results could not be archived to $RESULTS_ARCHIVE." >&2
            fi
        fi
    fi

    # --results-summary: print matching result (+ energy summary if monitored).
    emit_results_summary

    # Compute workload duration.
    end_time=$(date +%s)
    pipeline_duration=$((end_time - pipeline_start_time))
    pipeline_min=$((pipeline_duration/60))
    pipeline_sec=$((pipeline_duration%60))
    printf "Pipeline completed in %dm %02ds.\n" "$pipeline_min" "$pipeline_sec"

    # Compute full script duration, including editing, cleanup, PVC/config setup, and workload time.
    script_duration=$((end_time - script_start_time))
    script_min=$((script_duration/60))
    script_sec=$((script_duration%60))
    printf "Total script execution time: %dm %02ds.\n" "$script_min" "$script_sec"
}

stop_pipeline() {
    # Non-destructively cancel the latest Argo workflow and plain incremental Jobs.
    # Runtime PVCs, ConfigMaps, the static dataset volume, and saved results are preserved;
    # this is cancellation, not a resumable pause.
    local workflow_name

    # Find the newest pipeline-* workflow. Ignore errors so the empty case can be handled.
    workflow_name="$(latest_pipeline_workflow || true)"

    if [ -n "$workflow_name" ]; then
        argo stop -n "$NAMESPACE" "$workflow_name" || true
    fi
    kubectl delete -n "$NAMESPACE" -R -f "$PIPELINE_INCREMENTAL_DIR" \
        --ignore-not-found=true --wait=false
}

terminate_pipeline() {
    # Force-stop the latest Argo workflow (if any), then hard-reset: delete the EAER
    # ConfigMaps and wipe+recreate the pipeline's PVCs, regardless of whether a workflow
    # was found. Leaves the namespace ready for another `start` with no stale ConfigMaps,
    # Jobs, or PVC contents left over from this run.
    #
    # KNOWN LIMITATION: `argo terminate` only affects the Argo Workflow object. In
    # embedding mode the incremental worker Jobs are plain `kind: Job` manifests, not part
    # of any Workflow -- delete_pipeline_storage below does clean those up (it deletes
    # everything under $PIPELINE_INCREMENTAL_DIR), so terminate does fully tear down a
    # running incremental pipeline; it just doesn't go through Argo to do it.
    local workflow_name

    # Find the newest pipeline-* workflow. Ignore errors so the empty case can be handled.
    workflow_name="$(latest_pipeline_workflow || true)"

    # Forcefully terminate the workflow if it exists.
    if [ -n "$workflow_name" ]; then
        argo terminate -n "$NAMESPACE" "$workflow_name"
    else
        echo "No pipeline workflow found in namespace $NAMESPACE."
    fi

    delete_pipeline_configmaps
    delete_pipeline_storage true
}


# Default action when no explicit action is passed.
ACTION="start"

# Parse the optional first positional argument: start, stop, terminate, or help.
if [ $# -gt 0 ]; then
    case "$1" in
        start|stop|terminate)
            # Use the requested lifecycle action.
            ACTION="$1"
            # Remove the action from the remaining argument list.
            shift
            ;;
        -h|--help|-help|help)
            # Print help and exit immediately.
            usage
            exit 0
            ;;
    esac
fi

# Start mode: parse start-specific options, validate tools, then run start_pipeline.
if [ "$ACTION" = "start" ]; then
    # Parse options that follow `start`.
    while [ $# -gt 0 ]; do
        case "$1" in
            -c|--config)
                # `-c` and `--config` require a separate value.
                if [ $# -lt 2 ]; then
                    echo "Missing value for $1. Expected a path to a pipeline config YAML file."
                    exit 1
                fi
                CONFIG_PATH="$2"
                # Consume option name and value.
                shift 2
                ;;
            --config=*)
                # Support --config=path/to/config.yaml.
                CONFIG_PATH="${1#*=}"
                # Consume the single --config=value argument.
                shift
                ;;
            --energy-monitor)
                ENERGY_MONITOR=true
                if [ $# -ge 2 ] && { [ "$2" = "ecofloc" ] || [ "$2" = "alumet" ] || [ "$2" = "ecofloc-alumet" ]; }; then
                    ENERGY_MONITOR_TOOL="$2"
                    shift 2
                else
                    shift
                fi
                ;;
            --energy-monitor=*)
                ENERGY_MONITOR=true
                ENERGY_MONITOR_TOOL="${1#*=}"
                shift
                ;;
            --results-summary)
                # Print matching (+ energy, if monitored) summary after the workload ends.
                RESULTS_SUMMARY=true
                shift
                ;;
            --plan-only)
                PLAN_ONLY=true
                shift
                ;;
            --results-archive)
                if [ $# -lt 2 ]; then
                    echo "Missing value for --results-archive. Expected a path or user@host:/path."
                    exit 1
                fi
                RESULTS_ARCHIVE="$2"
                shift 2
                ;;
            --results-archive=*)
                RESULTS_ARCHIVE="${1#*=}"
                shift
                ;;
            -h|--help|-help|help)
                # Print help for the pipeline command.
                usage
                exit 0
                ;;
            *)
                # Reject unknown start options.
                echo "Unknown option for pipeline start: $1"
                echo "Use 'erctl pipeline --help' for usage information."
                exit 1
                ;;
        esac
    done

    # -c/--config is required: it is now the only way the file/mode is chosen, so there
    # is nothing sensible to default to.
    if [ -z "$CONFIG_PATH" ]; then
        echo "Missing required -c/--config <path>. Use 'erctl pipeline --help' for usage information."
        exit 1
    fi
    if [ ! -f "$CONFIG_PATH" ]; then
        echo "Config file not found: $CONFIG_PATH"
        exit 1
    fi
    # Resolve to an absolute path for the internal ConfigMap builder.
    CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" && pwd)/$(basename "$CONFIG_PATH")"

    # Required for Kubernetes resource operations.
    require_cmd kubectl

    # Required to check the batch workflow's final status.
    require_cmd python3
    require_cmd awk

    if [ "$PLAN_ONLY" = false ]; then
        # Required for submitting and observing Argo workflows.
        require_cmd argo
    fi

    # The energy monitor engine must exist when --energy-monitor is requested.
    if [ "$PLAN_ONLY" = false ] && [ "$ENERGY_MONITOR" = true ]; then
        case "$ENERGY_MONITOR_TOOL" in
            ecofloc)
                [ -f "$PROCESS_SCRIPT" ] || { echo "Energy monitor engine not found: $PROCESS_SCRIPT"; exit 1; }
                ;;
            alumet)
                [ -f "$ALUMET_SCRIPT" ] || { echo "Alumet adapter not found: $ALUMET_SCRIPT"; exit 1; }
                ;;
            ecofloc-alumet)
                [ -f "$PROCESS_SCRIPT" ] || { echo "Energy monitor engine not found: $PROCESS_SCRIPT"; exit 1; }
                [ -f "$ALUMET_SCRIPT" ] || { echo "Alumet adapter not found: $ALUMET_SCRIPT"; exit 1; }
                ;;
            *)
                echo "Unknown energy monitor: $ENERGY_MONITOR_TOOL (expected ecofloc, alumet, or ecofloc-alumet)"
                exit 1
                ;;
        esac
    fi

    # Execute the full start workflow.
    if [ "$PLAN_ONLY" = false ]; then
        trap 'interrupt_pipeline 130' INT
        trap 'interrupt_pipeline 143' TERM
    fi
    start_pipeline
elif [ "$ACTION" = "stop" ]; then
    # Stop accepts no extra arguments.
    if [ $# -gt 0 ]; then
        echo "Stop does not accept additional options."
        echo "Use 'erctl pipeline --help' for usage information."
        exit 1
    fi

    # Required for workflow lookup and Argo stop.
    require_cmd kubectl
    require_cmd argo
    require_cmd awk

    # Cancel active workloads without resetting their persistent state.
    stop_pipeline
elif [ "$ACTION" = "terminate" ]; then
    # Terminate accepts no extra arguments.
    if [ $# -gt 0 ]; then
        echo "Terminate does not accept additional options."
        echo "Use 'erctl pipeline --help' for usage information."
        exit 1
    fi

    # Required for workflow lookup and Argo terminate.
    require_cmd kubectl
    require_cmd argo
    require_cmd awk

    # Terminate the latest matching Argo workflow and run cleanup hooks.
    terminate_pipeline
else
    # This should only happen if ACTION is set programmatically to an unknown value.
    echo "Unknown pipeline action: $ACTION"
    exit 1
fi
