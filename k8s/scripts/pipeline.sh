#!/usr/bin/env bash

# Script to manage the pipeline

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

PIPELINE_PATH=/home/kevin/k8s-python-llm/k8s/argo/pipeline.yaml
NODE=server2-labo
PVC_MANIFESTS=/home/kevin/k8s-python-llm/k8s/argo/pvc-manifests/
PIPELINE_MODE="embedding"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

start_pipeline() {
    CONFIG_PATH=/home/kevin/k8s-python-llm/code/Energy-Aware-Entity-Resolution/config/examples/config-${PIPELINE_MODE}.yaml
    timeout 5 kubectl get po -n argo -o name | grep '^pod/pipeline-' | xargs kubectl delete -n argo || true
    timeout 5 kubectl delete pv -n argo --all || true
    timeout 5 kubectl delete pvc -n argo --all || true
    timeout 5 kubectl get po -n argo -o name | grep '^pod/kafka-server' | xargs kubectl delete -n argo || true
    kubectl apply -n argo -f $PVC_MANIFESTS
    kubectl patch pvc pipeline-kafka-data-claim -n argo \
        -p "{\"metadata\":{\"annotations\":{\"volume.kubernetes.io/selected-node\":\"$NODE\"}}}"
    kubectl patch pvc pipeline-data-claim -n argo \
        -p "{\"metadata\":{\"annotations\":{\"volume.kubernetes.io/selected-node\":\"$NODE\"}}}"
    $SCRIPT_DIR/erctl.sh dataset
    nano $CONFIG_PATH
    kubectl get cm -n argo -o name | grep '^configmap/eaer-' | xargs kubectl delete -n argo
    kubectl delete cm -n argo er-pipeline-config
    $SCRIPT_DIR/erctl.sh configmaps $PIPELINE_MODE
    argo submit -n argo $PIPELINE_PATH
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
                if [ $# -lt 1 ]; then
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

