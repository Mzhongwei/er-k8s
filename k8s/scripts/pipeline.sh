#!/usr/bin/env bash

# Manage the Kubernetes/Argo pipeline lifecycle.
# This script is normally called through:
#   k8s/scripts/erctl.sh pipeline ...

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

# Directory containing PVC manifests that must exist before jobs/workflows run.
PVC_MANIFESTS="$K8S_DIR/pvc-manifests"

# Directory containing config-embedding.yaml and config-bert.yaml.
PIPELINE_CONFIG_DIR="$ROOT_DIR/code/Energy-Aware-Entity-Resolution/config/examples"

# Kubernetes namespace used by Argo and the pipeline resources.
NAMESPACE="argo"

# Default node name kept for older/manual PVC patching logic.
# It is currently only used by the commented kubectl patch below.
NODE=server2-labo

# Default config family. Can be overridden by `-m embedding` or `-m bert`.
PIPELINE_MODE="embedding"

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

    # Used in incremental evaluation mode, where evaluation starts after decision-making.
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

    # Delete old pipeline pods if any are still present.
    # `timeout 5` prevents cleanup from blocking the whole run.
    # `|| true` makes cleanup best-effort under `set -e`.
    timeout 5 kubectl get po -n "$NAMESPACE" -o name | grep '^pod/pipeline-' | xargs kubectl delete -n "$NAMESPACE" || true

    # Delete old Jobs, Argo workflows, PersistentVolumes and PVCs
    kubectl delete -n "$NAMESPACE" -f "$PIPELINE_INCREMENTAL_DIR" --ignore-not-found=true || true
    timeout 5 kubectl get wf -n "$NAMESPACE" --no-headers -o custom-columns=NAME:.metadata.name | grep '^pipeline-' | xargs -r argo delete -n "$NAMESPACE" || true
    timeout 5 kubectl delete pv --all || true
    timeout 5 kubectl delete pvc -n "$NAMESPACE" --all || true

    # Optional Kafka cleanup, currently disabled.
    # timeout 5 kubectl get po -n "$NAMESPACE" -o name | grep '^pod/kafka-server' | xargs kubectl delete -n "$NAMESPACE" || true

    # Recreate PVCs needed by the pipeline.
    kubectl apply -n "$NAMESPACE" -f "$PVC_MANIFESTS"

    # Optional manual node pinning for a Kafka PVC, currently disabled.
    # kubectl patch pvc pipeline-kafka-data-claim -n "$NAMESPACE" \
    #     -p "{\"metadata\":{\"annotations\":{\"volume.kubernetes.io/selected-node\":\"$NODE\"}}}"

    # Sync static dataset files into the data PVC.
    "$SCRIPT_DIR/erctl.sh" dataset

    # Let the user edit the selected pipeline config before creating ConfigMaps.
    nano "$config_path"

    # Read mode from the edited config. This decides batch vs incremental execution.
    config_mode="$(extract_mode_from_config "$config_path")"

    # Stop early if the config has no mode field.
    if [ -z "$config_mode" ]; then
        echo "Unable to read mode from config file: $config_path"
        exit 1
    fi

    # Delete old EAER script ConfigMaps to avoid stale mounted code, and the previous pipeline config ConfigMap.
    kubectl get cm -n "$NAMESPACE" -o name | grep '^configmap/eaer-' | xargs kubectl delete -n "$NAMESPACE" || true
    kubectl delete cm -n "$NAMESPACE" er-pipeline-config || true

    # ConfigMaps
    "$SCRIPT_DIR/erctl.sh" configmaps "$PIPELINE_MODE"

    # Measure only the actual workload execution time from this point.
    pipeline_start_time=$(date +%s)

    # Track whether any supported execution branch was selected.
    executed_pipeline=false

    # Batch branch:
    # - embedding-training modes run through Argo
    # - all bert modes run through Argo
    if [[ "$config_mode" == *training* || "$config_mode" == *bert* ]]; then
        # Submit the generated Argo Workflow and stream status until completion.
        argo submit -n "$NAMESPACE" "$PIPELINE_BATCH_PATH" --watch

        # For BERT evaluation, print result lines from the latest workflow logs.
        if [[ "$config_mode" == *b_evaluation* ]]; then
            argo logs -n "$NAMESPACE" @latest | grep -F '[RESULT]'
        fi

        # Mark that a valid execution branch was used.
        executed_pipeline=true
    fi

    # Incremental branch:
    # - embedding inference runs as long-lived/worker-like Kubernetes Jobs
    if [[ "$config_mode" == *embedding* && "$config_mode" == *inference* ]]; then
        # Temporarily disable evaluation so the main incremental chain starts first.
        mv "$PIPELINE_INCREMENTAL_DIR/evaluation.yaml" "$PIPELINE_INCREMENTAL_DIR/evaluation.yaml.DISABLED"

        # Apply all incremental Job manifests except evaluation.yaml.
        kubectl apply -n "$NAMESPACE" -f "$PIPELINE_INCREMENTAL_DIR"

        # Restore the evaluation manifest locally after applying the other jobs.
        mv "$PIPELINE_INCREMENTAL_DIR/evaluation.yaml.DISABLED" "$PIPELINE_INCREMENTAL_DIR/evaluation.yaml"

        # If the mode asks for evaluation, run it after decision-making completes.
        if [[ "$config_mode" == *evaluation* ]]; then
            echo "Waiting for decision-making job to complete before starting evaluation..."

            # Wait for decision-making to finish before launching evaluation.
            wait_for_job_completion decision-making

            # Start the evaluation Job separately.
            kubectl apply -n "$NAMESPACE" -f "$PIPELINE_INCREMENTAL_DIR/evaluation.yaml"

            # Wait for evaluation to finish.
            wait_for_job_completion evaluation

            # Print final evaluation result lines.
            kubectl logs -n "$NAMESPACE" -l app=evaluation --tail=-1 | grep -F '[Result]'
        fi

        # Mark that a valid execution branch was used.
        executed_pipeline=true
    fi

    # Reject unknown or unsupported mode values.
    if [[ ! "$executed_pipeline" ]]; then
        echo "Unsupported mode in $config_path: $config_mode"
        exit 1
    fi

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
    # Terminate the latest Argo workflow and then clean up related resources.
    local workflow_name

    # Find the newest pipeline-* workflow. Ignore errors so the empty case can be handled.
    workflow_name="$(latest_pipeline_workflow || true)"

    # Forcefully terminate the workflow if it exists.
    if [ -n "$workflow_name" ]; then
        argo terminate -n "$NAMESPACE" "$workflow_name"
    else
        echo "No pipeline workflow found in namespace $NAMESPACE."
    fi

    # These cleanup functions are referenced here but are not defined in this file.
    # If terminate is used, they must exist in the runtime environment or be added.
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

    # Currently not used in this file, but kept as a legacy requirement.
    require_cmd shuf

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
