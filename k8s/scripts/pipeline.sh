#!/usr/bin/env bash

# Script to manage the pipeline

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
K8S_DIR="$ROOT_DIR/k8s"
PIPELINE_BATCH_PATH="$K8S_DIR/pipeline/batch/pipeline.yaml"
PIPELINE_INCREMENTAL_DIR="$K8S_DIR/pipeline/incremental"
PVC_MANIFESTS="$K8S_DIR/pvc-manifests"
PIPELINE_CONFIG_DIR="$ROOT_DIR/code/Energy-Aware-Entity-Resolution/config/examples"
NAMESPACE="argo"
NODE=server2-labo
PIPELINE_MODE="embedding"

usage() {
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
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Required command not found: $cmd"
        exit 1
    fi
}

latest_pipeline_workflow() {
    kubectl get wf -n "$NAMESPACE" --no-headers -o custom-columns=NAME:.metadata.name,CREATED:.metadata.creationTimestamp \
        | awk '$1 ~ /^pipeline-/ { print $1 "\t" $2 }' \
        | sort -k2,2 \
        | tail -n 1 \
        | cut -f1
}

wait_for_job_completion() {
    local job_name="$1"

    kubectl wait -n "$NAMESPACE" --for=condition=complete "job/$job_name" --timeout=30m
}

extract_mode_from_config() {
    local config_path="$1"

    awk -F': *' '
        /^[[:space:]]*mode[[:space:]]*:/ {
            mode = $2
            gsub(/^"|"$/, "", mode)
            print mode
            exit
        }
    ' "$config_path"
}

start_pipeline() {
    local config_path
    local config_mode

    config_path="$PIPELINE_CONFIG_DIR/config-${PIPELINE_MODE}.yaml"
    start_time=$(date +%s)

    timeout 5 kubectl get po -n "$NAMESPACE" -o name | grep '^pod/pipeline-' | xargs kubectl delete -n "$NAMESPACE" || true
        kubectl delete -n "$NAMESPACE" -f "$PIPELINE_INCREMENTAL_DIR" --ignore-not-found=true || true
    timeout 5 kubectl delete pv --all || true
    timeout 5 kubectl delete pvc -n "$NAMESPACE" --all || true
    timeout 5 kubectl get po -n "$NAMESPACE" -o name | grep '^pod/kafka-server' | xargs kubectl delete -n "$NAMESPACE" || true

    kubectl apply -n "$NAMESPACE" -f "$PVC_MANIFESTS"
    kubectl patch pvc pipeline-kafka-data-claim -n "$NAMESPACE" \
        -p "{\"metadata\":{\"annotations\":{\"volume.kubernetes.io/selected-node\":\"$NODE\"}}}"

    "$SCRIPT_DIR/erctl.sh" dataset

    nano "$config_path"
    config_mode="$(extract_mode_from_config "$config_path")"
    if [ -z "$config_mode" ]; then
        echo "Unable to read mode from config file: $config_path"
        exit 1
    fi

    kubectl get cm -n "$NAMESPACE" -o name | grep '^configmap/eaer-' | xargs kubectl delete -n "$NAMESPACE" || true
    kubectl delete cm -n "$NAMESPACE" er-pipeline-config || true
    "$SCRIPT_DIR/erctl.sh" configmaps "$PIPELINE_MODE"

    if [[ "$config_mode" == *training* || "$config_mode" == *bert* ]]; then
        argo submit -n "$NAMESPACE" "$PIPELINE_BATCH_PATH" --watch
        if [[ "$config_mode" == *b_evaluation* ]]; then
            argo logs -n "$NAMESPACE" @latest | grep -F '[RESULT]'
        fi
    fi
    if [[ "$config_mode" == *embedding* && "$config_mode" == *inference* ]]; then
        mv "$PIPELINE_INCREMENTAL_DIR/evaluation.yaml" "$PIPELINE_INCREMENTAL_DIR/evaluation.yaml.DISABLED"
        kubectl apply -n "$NAMESPACE" -f "$PIPELINE_INCREMENTAL_DIR"
        mv "$PIPELINE_INCREMENTAL_DIR/evaluation.yaml.DISABLED" "$PIPELINE_INCREMENTAL_DIR/evaluation.yaml"
        if [[ "$config_mode" == *evaluation* ]]; then
            echo "Waiting for decision-making job to complete before starting evaluation..."
            wait_for_job_completion decision-making
            kubectl apply -n "$NAMESPACE" -f "$PIPELINE_INCREMENTAL_DIR/evaluation.yaml"
            wait_for_job_completion evaluation
            kubectl logs -n "$NAMESPACE" -l app=evaluation --tail=-1 | grep -F '[Result]'
        fi
    else
        echo "Unsupported mode in $config_path: $config_mode"
        exit 1
    fi

    end_time=$(date +%s)
    duration=$((end_time - start_time))
    echo "Pipeline completed in $duration seconds."
}

stop_pipeline() {
    local workflow_name

    workflow_name="$(latest_pipeline_workflow || true)"
    if [ -z "$workflow_name" ]; then
        echo "No pipeline workflow found in namespace $NAMESPACE."
        exit 0
    fi

    argo stop -n "$NAMESPACE" "$workflow_name"
}

terminate_pipeline() {
    local workflow_name

    workflow_name="$(latest_pipeline_workflow || true)"
    if [ -n "$workflow_name" ]; then
        argo terminate -n "$NAMESPACE" "$workflow_name"
    else
        echo "No pipeline workflow found in namespace $NAMESPACE."
    fi

    delete_pipeline_configmaps
    delete_pipeline_storage true
}



ACTION="start"
RANDOM_VERSION_NAME=false
CREATE_CONFIGMAPS=false
SYNC_DATASET=false
CLEAR_KAFKA_STORAGE=false

if [ $# -gt 0 ]; then
    case "$1" in
        start|stop|terminate)
            ACTION="$1"
            shift
            ;;
        -h|--help|-help|help)
            usage
            exit 0
            ;;
    esac
fi

if [ "$ACTION" = "start" ]; then
    while [ $# -gt 0 ]; do
        case "$1" in
            -m|--mode)
                if [ $# -lt 2 ]; then
                    echo "Missing value for $1. Expected 'embedding' or 'bert'."
                    exit 1
                fi
                PIPELINE_MODE="$2"
                shift 2
                ;;
            --mode=*)
                PIPELINE_MODE="${1#*=}"
                shift
                ;;
            -h|--help|-help|help)
                usage
                exit 0
                ;;
            *)
                echo "Unknown option for pipeline start: $1"
                echo "Use 'erctl pipeline --help' for usage information."
                exit 1
                ;;
        esac
    done

    if [[ "$PIPELINE_MODE" != "embedding" && "$PIPELINE_MODE" != "bert" ]]; then
        echo "Invalid mode: $PIPELINE_MODE. Use 'embedding' or 'bert'."
        exit 1
    fi

    require_cmd kubectl
    require_cmd argo
    require_cmd awk
    require_cmd shuf
    start_pipeline
elif [ "$ACTION" = "stop" ]; then
    if [ $# -gt 0 ]; then
        echo "Stop does not accept additional options."
        echo "Use 'erctl pipeline --help' for usage information."
        exit 1
    fi

    require_cmd kubectl
    require_cmd argo
    require_cmd awk
    stop_pipeline
elif [ "$ACTION" = "terminate" ]; then
    if [ $# -gt 0 ]; then
        echo "Terminate does not accept additional options."
        echo "Use 'erctl pipeline --help' for usage information."
        exit 1
    fi

    require_cmd kubectl
    require_cmd argo
    require_cmd awk
    terminate_pipeline
else
    echo "Unknown pipeline action: $ACTION"
    exit 1
fi