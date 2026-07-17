#!/usr/bin/env bash

# Manage the Kubernetes/Argo pipeline lifecycle.
# This script is normally called through:
#   k8s/scripts/erctl.sh pipeline ...
#
# Lifecycle overview:
#   start     Full setup + run. Compiles manifests, wipes prior Jobs/Workflows/PVCs for
#             this namespace, recreates PVCs, syncs the dataset, (re)creates ConfigMaps,
#             then dispatches on config-{embedding,bert}.yaml's `mode:` field:
#               - embedding-training-inference-evaluation: runs the batch training Argo
#                 Workflow to completion first (seeds graph/embedding/index models), then
#                 applies the incremental worker Jobs (producer/consumer/normalization/.../
#                 evaluation) as plain `kind: Job` manifests -- NOT part of any Argo
#                 Workflow. These run concurrently and stream until the input exhausts.
#               - bert-training-evaluation: runs entirely as one Argo Workflow (no
#                 incremental phase).
#   stop      Best-effort pause: finds the newest `pipeline-*` Argo Workflow and asks Argo
#             to stop it. Only ever affects the batch training Workflow -- the incremental
#             worker Jobs from an embedding run are plain Kubernetes Jobs, invisible to
#             `argo stop`, and keep running. See the note in stop_pipeline().
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
PIPELINE_INCREMENTAL_INIT_PATH="$PIPELINE_INCREMENTAL_DIR/bootstrap/incremental_init.yaml"
PIPELINE_INCREMENTAL_WORKERS_DIR="$PIPELINE_INCREMENTAL_DIR/workers"

# Directory containing PVC manifests that must exist before jobs/workflows run.
PVC_MANIFESTS="$K8S_DIR/pvc-manifests"

# Directory containing config-embedding.yaml and config-bert.yaml.
PIPELINE_CONFIG_DIR="$ROOT_DIR/code/Energy-Aware-Entity-Resolution/config/examples"

# Kubernetes namespace used by Argo and the pipeline resources. All `kubectl`/`argo` calls
# below are scoped to this namespace, including PVC create/delete -- PersistentVolumeClaims
# are namespaced, so `kubectl delete pvc -n "$NAMESPACE" --all` only ever touches this
# namespace's claims, never another namespace's. Note this value must match the hardcoded
# `namespace: argo` in every k8s/pvc-manifests/*.yaml: `kubectl apply -n X -f file.yaml`
# errors out if X differs from the namespace already set in the manifest, so changing this
# variable alone is not enough to move the pipeline to a different namespace.
NAMESPACE="argo"

# Default node name kept for older/manual PVC patching logic.
# It is currently only used by the commented kubectl patch below.
NODE=server2-labo

# Default config family. Can be overridden by `-m embedding` or `-m bert`.
PIPELINE_MODE="embedding"

# The pipeline intentionally supports exactly these two end-to-end business modes.
EMBEDDING_PIPELINE_MODE="embedding-training-inference-evaluation"
BERT_PIPELINE_MODE="bert-training-evaluation"

# Human-editable scheduling source file. compiler.py reads this file.
SCHEDULING_CONFIG_PATH="$K8S_DIR/scripts/scheduling.yaml"

usage() {
    # Print command help without executing any cluster operation.
    cat << 'EOF'
Usage: erctl pipeline [start|stop|terminate] [options]
Manage the pipeline.
Options:
    start                    Start the pipeline (default)
    stop                     Stop the latest pipeline
    terminate                Terminate the latest pipeline and clean up resources
    -m, --mode MODE          ConfigMap mode for pipeline config (`embedding` or `bert`) default: `embedding`
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

wait_for_job_completion() {
    # Wait for a Kubernetes Job to finish successfully.
    local job_name="$1"

    # Used to gate incremental-init (buffer cleanup must finish before workers start) and
    # to know when the evaluation worker has fully drained the decision-making stream.
    #
    # KNOWN LIMITATION: the same flat 30m timeout is used for both callers, even though
    # they have very different expected durations -- incremental-init only clears buffer
    # directories (seconds), while evaluation blocks on the entire incremental stream
    # (producer/consumer/normalization/.../decision-making) finishing, which has no upper
    # bound. A genuinely long-running stream makes this `kubectl wait` time out and abort
    # start_pipeline even though decision-making/evaluation are still legitimately working.
    # Left as-is for now (30m accepted as a stopgap); a real fix would need separate,
    # per-caller timeouts.
    kubectl wait -n "$NAMESPACE" --for=condition=complete "job/$job_name" --timeout=30m
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
    # Delete pipeline pods, incremental Jobs, Argo workflows and PVCs in this namespace.
    # PersistentVolumes are cluster-scoped (no namespace), so they are deliberately NOT
    # deleted here -- `kubectl delete pv --all` would remove every PV in the cluster,
    # including ones bound to unrelated namespaces. Deleting the PVCs below is enough:
    # dynamically-provisioned PVs follow the storage class's reclaim policy (nfs-client
    # defaults to Delete), so they get cleaned up as a consequence of their PVC going away,
    # scoped correctly to just this namespace's claims.
    #
    # recreate=true also re-applies the PVC manifests afterward, leaving empty PVCs ready
    # for immediate reuse (start_pipeline's own cleanup pass wants this); recreate=false
    # (the default, used by terminate_pipeline) just tears down and stops there.
    local recreate="${1:-false}"

    # `timeout 5` prevents cleanup from blocking the whole run; `|| true` makes it
    # best-effort under `set -e`; `xargs -r` skips running kubectl at all when nothing
    # matched, instead of invoking it with no resource name.
    timeout 5 kubectl get po -n "$NAMESPACE" -o name | grep '^pod/pipeline-' | xargs -r kubectl delete -n "$NAMESPACE" || true
    kubectl delete -n "$NAMESPACE" -R -f "$PIPELINE_INCREMENTAL_DIR" --ignore-not-found=true || true
    timeout 5 kubectl get wf -n "$NAMESPACE" --no-headers -o custom-columns=NAME:.metadata.name | grep '^pipeline-' | xargs -r argo delete -n "$NAMESPACE" || true
    timeout 5 kubectl delete pvc -n "$NAMESPACE" --all || true

    if [ "$recreate" = true ]; then
        kubectl apply -n "$NAMESPACE" -f "$PVC_MANIFESTS"
    fi
}

start_pipeline() {
    # Start either the batch Argo workflow or the incremental Kubernetes Jobs.
    local config_path
    local config_mode

    # Select config-embedding.yaml or config-bert.yaml based on PIPELINE_MODE.
    config_path="$PIPELINE_CONFIG_DIR/config-${PIPELINE_MODE}.yaml"

    # Measure total script wall-clock time.
    script_start_time=$(date +%s)

    # Let the user edit scheduling rules before compiling executable manifests.
    nano "$SCHEDULING_CONFIG_PATH"

    # Compile 
    "$SCRIPT_DIR/erctl.sh" compile

    # Wipe pods/Jobs/workflows/PVCs left over from a previous run, then recreate empty
    # PVCs ready for this one. See delete_pipeline_storage() for exactly what this deletes.
    delete_pipeline_storage true

    # Optional Kafka cleanup, currently disabled.
    # timeout 5 kubectl get po -n "$NAMESPACE" -o name | grep '^pod/kafka-server' | xargs kubectl delete -n "$NAMESPACE" || true

    # Optional manual node pinning for a Kafka PVC, currently disabled.
    # kubectl patch pvc pipeline-kafka-data-claim -n "$NAMESPACE" \
    #     -p "{\"metadata\":{\"annotations\":{\"volume.kubernetes.io/selected-node\":\"$NODE\"}}}"

    # Sync static dataset files into the data PVC. Pass PIPELINE_MODE (embedding/bert,
    # already known from -m/--mode) so sync-data-pvc.sh only requires the file group this
    # run actually needs (tableA/tableB/matches.txt for embedding, train/test/valid.csv
    # for bert) instead of demanding both regardless of which mode is about to run.
    "$SCRIPT_DIR/erctl.sh" dataset "$PIPELINE_MODE"

    # Let the user edit the selected pipeline config before creating ConfigMaps.
    nano "$config_path"

    # Read mode from the edited config. This decides batch vs incremental execution.
    config_mode="$(extract_mode_from_config "$config_path")"

    # Stop early if the config has no mode field.
    if [ -z "$config_mode" ]; then
        echo "Unable to read mode from config file: $config_path"
        exit 1
    fi

    # Guard against PIPELINE_MODE (from -m/--mode) and config_mode (the mode: string just
    # read back out of the file the user may have hand-edited above) disagreeing. The
    # dataset sync (erctl dataset "$PIPELINE_MODE" above) and the ConfigMap family
    # (erctl configmaps "$PIPELINE_MODE" below) are both already prepared for
    # PIPELINE_MODE, so a mismatch here (e.g. `-m embedding` but the file's mode: was
    # hand-edited to a bert-* value) would otherwise only surface later, as a confusing
    # failure deep inside whichever worker first needs a field the wrong dataset doesn't
    # have -- catch it here instead, before ConfigMaps/workloads are touched.
    case "$PIPELINE_MODE" in
        embedding)
            if [[ "$config_mode" != embedding-* ]]; then
                echo "config_mode '$config_mode' in $config_path doesn't match -m embedding" \
                     "(the dataset and ConfigMaps were already prepared for embedding)."
                exit 1
            fi
            ;;
        bert)
            if [[ "$config_mode" != bert-* ]]; then
                echo "config_mode '$config_mode' in $config_path doesn't match -m bert" \
                     "(the dataset and ConfigMaps were already prepared for bert)."
                exit 1
            fi
            ;;
    esac

    # Delete old EAER script ConfigMaps to avoid stale mounted code, and the previous
    # pipeline config ConfigMap.
    delete_pipeline_configmaps

    # ConfigMaps
    "$SCRIPT_DIR/erctl.sh" configmaps "$PIPELINE_MODE"

    # Measure only the actual workload execution time from this point.
    pipeline_start_time=$(date +%s)

    case "$config_mode" in
        "$EMBEDDING_PIPELINE_MODE")
            # 1. Clear stale incremental buffers before either phase starts.
            echo "Running incremental-init (buffer cleanup)..."
            kubectl apply -n "$NAMESPACE" -f "$PIPELINE_INCREMENTAL_INIT_PATH"
            wait_for_job_completion incremental-init

            # 2. Build the graph, embedding model and feature index in Argo.
            argo submit -n "$NAMESPACE" "$PIPELINE_BATCH_PATH" -p mode="$config_mode" --watch

            # 3. Start producer, consumer and all processing workers together, including
            # evaluation. decision_making.py writes a per-window predicted-matching
            # snapshot and references it in its buffer event, so evaluation.py -- running
            # concurrently -- evaluates the exact graph produced for that window instead of
            # whatever a shared path happens to hold, matching the business design (evaluate
            # after every decision, report overwritten so only the latest is kept). It exits
            # on its own once it sees decision-making's EOS.
            for worker_manifest in "$PIPELINE_INCREMENTAL_WORKERS_DIR"/*.yaml; do
                kubectl apply -n "$NAMESPACE" -f "$worker_manifest"
            done

            echo "Waiting for evaluation job to complete..."
            wait_for_job_completion evaluation
            kubectl logs -n "$NAMESPACE" -l app=evaluation --tail=-1 | grep -F '[Result]' | tail -n 1
            ;;

        "$BERT_PIPELINE_MODE")
            # The Argo main DAG routes every bert mode to the fixed
            # bert-training-evaluation-dag sequence.
            argo submit -n "$NAMESPACE" "$PIPELINE_BATCH_PATH" -p mode="$config_mode" --watch
            argo logs -n "$NAMESPACE" @latest | grep -F '[RESULT]' | tail -n 1
            ;;

        *)
            echo "Unsupported mode in $config_path: $config_mode"
            echo "Supported modes: $EMBEDDING_PIPELINE_MODE, $BERT_PIPELINE_MODE"
            exit 1
            ;;
    esac

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
    # Stop the latest Argo workflow created by this pipeline.
    #
    # BUG / KNOWN LIMITATION: latest_pipeline_workflow only looks at Argo Workflow objects
    # (`kubectl get wf`), and `argo stop` only ever acts on those. In embedding mode, the
    # incremental worker Jobs (producer/consumer/normalization/.../evaluation) are plain
    # `kind: Job` manifests applied directly via `kubectl apply` in start_pipeline -- they
    # are never registered with Argo at all. By the time those workers are running, the
    # batch training Workflow they depended on has usually already completed (step 2 in
    # start_pipeline blocks on `argo submit --watch` before step 3 applies the workers), so
    # there is nothing left for `argo stop` to act on. In practice this command does not
    # stop a running embedding/incremental pipeline; use `kubectl delete job -n "$NAMESPACE"
    # -l app=<job-name>` (or delete the whole $PIPELINE_INCREMENTAL_DIR) to actually stop
    # the incremental workers.
    local workflow_name

    # Find the newest pipeline-* workflow. Ignore errors so the empty case can be handled.
    workflow_name="$(latest_pipeline_workflow || true)"

    # If no workflow exists, stopping is a no-op.
    if [ -z "$workflow_name" ]; then
        echo "No pipeline workflow found in namespace $NAMESPACE."
        exit 0
    fi

    # Ask Argo to stop the workflow gracefully.
    argo stop -n "$NAMESPACE" "$workflow_name"
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

# Currently unused feature flags kept from an earlier version of the script.
RANDOM_VERSION_NAME=false
CREATE_CONFIGMAPS=false
SYNC_DATASET=false
CLEAR_KAFKA_STORAGE=false

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
            -m|--mode)
                # `-m` and `--mode` require a separate value.
                if [ $# -lt 2 ]; then
                    echo "Missing value for $1. Expected 'embedding' or 'bert'."
                    exit 1
                fi
                # Select which config file and ConfigMap family to use.
                PIPELINE_MODE="$2"
                # Consume option name and value.
                shift 2
                ;;
            --mode=*)
                # Support --mode=embedding and --mode=bert.
                PIPELINE_MODE="${1#*=}"
                # Consume the single --mode=value argument.
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

    # Only two config families are supported by configmaps.sh.
    if [[ "$PIPELINE_MODE" != "embedding" && "$PIPELINE_MODE" != "bert" ]]; then
        echo "Invalid mode: $PIPELINE_MODE. Use 'embedding' or 'bert'."
        exit 1
    fi

    # Required for Kubernetes resource operations.
    require_cmd kubectl

    # Required for submitting/stopping/terminating Argo workflows.
    require_cmd argo

    # Required by helper functions in this script.
    require_cmd awk

    # Execute the full start workflow.
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

    # Stop the latest matching Argo workflow.
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
