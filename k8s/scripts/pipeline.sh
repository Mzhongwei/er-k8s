#!/usr/bin/env bash

# Script to manage the pipeline

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

NAMESPACE="${EAER_PIPELINE_NAMESPACE:-argo}"
PIPELINE_MANIFEST="${ROOT_DIR}/k8s/argo/pipeline.yaml"
PVC_MANIFEST_DIR="${ROOT_DIR}/k8s/argo/pvc-manifests"
PIPELINE_CONFIGMAP="er-pipeline-config"
VERSION_NAMES_FILE="${SCRIPT_DIR}/pipeline-version-names.txt"

PVC_NAMES=(
    pipeline-bert-model-claim
    pipeline-buffer-data-claim
    pipeline-data-claim
    pipeline-decision-evaluation-cache-claim
    pipeline-embedding-model-cache-claim
    pipeline-feature-index-cache-claim
    pipeline-graph-cache-claim
    pipeline-reports-claim
)

usage() {
    cat << 'EOF'
Usage: erctl pipeline [start|stop|terminate] [options]
Manage the pipeline.
Options:
    start                    Start the pipeline (default)
    stop                     Stop the latest pipeline
    terminate                Terminate the latest pipeline and clean up resources
    -m, --mode MODE          ConfigMap mode for pipeline config (`embedding` or `bert`) default: `embedding`
    -r, --random-version-name    Pick a random version name from the local pool
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

choose_random_version_name() {
    local names_file="$1"
    local available_count=""
    local chosen_name=""
    local temp_file=""

    if [ ! -f "$names_file" ]; then
        echo "Version name pool not found: $names_file"
        exit 1
    fi

    available_count="$(grep -vc '^[[:space:]]*$' "$names_file" || true)"
    if [ "$available_count" -le 0 ]; then
        echo "No version names left in $names_file"
        exit 1
    fi

    chosen_name="$(grep -v '^[[:space:]]*$' "$names_file" | shuf -n 1)"
    temp_file="$(mktemp)"

    awk -v choice="$chosen_name" '
        BEGIN { removed = 0 }
        /^[[:space:]]*$/ { print; next }
        !removed && $0 == choice { removed = 1; next }
        { print }
        END { if (!removed) exit 1 }
    ' "$names_file" > "$temp_file"

    mv "$temp_file" "$names_file"
    printf '%s\n' "$chosen_name"
}

delete_pipeline_configmaps() {
    local configmap_names=()
    local configmap_ref=""

    while IFS= read -r configmap_ref; do
        [ -n "$configmap_ref" ] || continue
        configmap_names+=("${configmap_ref#configmap/}")
    done < <(kubectl get cm -n "$NAMESPACE" -o name | grep '^configmap/eaer-' || true)

    if [ "${#configmap_names[@]}" -gt 0 ]; then
        kubectl delete cm -n "$NAMESPACE" "${configmap_names[@]}"
    fi

    kubectl delete cm -n "$NAMESPACE" "$PIPELINE_CONFIGMAP" --ignore-not-found=true
}

delete_pipeline_storage() {
    local pvc=""
    local pv=""
    local claimed_pv=""
    local pod=""
    local pod_pvc=""
    local pods_to_delete=()
    local pv_names=()

    # Delete any pod that still references one of the target PVCs.
    # This avoids PVCs getting stuck in Terminating due to lingering Completed pods.
    while IFS= read -r pod; do
        [ -n "$pod" ] || continue

        while IFS= read -r pod_pvc; do
            [ -n "$pod_pvc" ] || continue
            for pvc in "${PVC_NAMES[@]}"; do
                if [ "$pod_pvc" = "$pvc" ]; then
                    pods_to_delete+=("$pod")
                    break
                fi
            done
        done < <(kubectl get pod -n "$NAMESPACE" "$pod" -o jsonpath='{range .spec.volumes[*]}{.persistentVolumeClaim.claimName}{"\n"}{end}' 2>/dev/null || true)
    done < <(kubectl get pods -n "$NAMESPACE" --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null || true)

    if [ "${#pods_to_delete[@]}" -gt 0 ]; then
        mapfile -t pods_to_delete < <(printf '%s\n' "${pods_to_delete[@]}" | awk '!seen[$0]++')
        kubectl delete pod -n "$NAMESPACE" "${pods_to_delete[@]}" --ignore-not-found=true --wait=false
    fi

    for pvc in "${PVC_NAMES[@]}"; do
        pv="$(kubectl get pvc -n "$NAMESPACE" "$pvc" -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)"
        if [ -n "$pv" ]; then
            pv_names+=("$pv")
        fi
    done

    while IFS= read -r claimed_pv; do
        [ -n "$claimed_pv" ] || continue
        pv_names+=("$claimed_pv")
    done < <(
        kubectl get pv -o jsonpath="{range .items[?(@.spec.claimRef.namespace=='${NAMESPACE}')]}{.metadata.name}{'\n'}{end}" 2>/dev/null || true
    )

    if [ "${#pv_names[@]}" -gt 0 ]; then
        mapfile -t pv_names < <(printf '%s\n' "${pv_names[@]}" | awk '!seen[$0]++')
    fi

    kubectl delete pvc -n "$NAMESPACE" "${PVC_NAMES[@]}" --ignore-not-found=true

    if [ "${#pv_names[@]}" -gt 0 ]; then
        kubectl delete pv "${pv_names[@]}" --ignore-not-found=true
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
    local version_name=""

    if [ "$RANDOM_VERSION_NAME" = true ]; then
        version_name="$(choose_random_version_name "$VERSION_NAMES_FILE")"
        export EAER_PIPELINE_VERSION_NAME="$version_name"
        echo "Using pipeline version name: $version_name"
    fi

    delete_pipeline_configmaps
    delete_pipeline_storage

    if [ ! -d "$PVC_MANIFEST_DIR" ]; then
        echo "PVC manifest directory not found: $PVC_MANIFEST_DIR"
        exit 1
    fi

    kubectl apply -n "$NAMESPACE" -f "$PVC_MANIFEST_DIR"
    bash "${SCRIPT_DIR}/erctl" dataset
    bash "${SCRIPT_DIR}/erctl" configmaps "$PIPELINE_MODE"
    argo submit -n "$NAMESPACE" "$PIPELINE_MANIFEST"
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
    delete_pipeline_storage
}

ACTION="start"
RANDOM_VERSION_NAME=false
PIPELINE_MODE="embedding"

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
            -r|--random-version-name|--random-name)
                RANDOM_VERSION_NAME=true
                ;;
            -m|--mode)
                if [ $# -lt 2 ]; then
                    echo "Missing value for $1. Expected 'embedding' or 'bert'."
                    exit 1
                fi
                PIPELINE_MODE="$2"
                shift
                ;;
            --mode=*)
                PIPELINE_MODE="${1#*=}"
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
        shift
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

