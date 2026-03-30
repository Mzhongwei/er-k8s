#!/bin/bash

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

# This script starts Minikube, applies manifests, and creates/updates ConfigMaps.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SERVICES_DIR="${ROOT_DIR}/k8s/services"
DEPLOYMENTS_DIR="${ROOT_DIR}/k8s/deployments"
PYTHON_SCRIPTS_DIR="${ROOT_DIR}/code/python_files"
PERSISTENT_VOLUMES_DIR="${ROOT_DIR}/k8s/persistent-volumes"
PERSISTENT_VOLUME_CLAIMS_DIR="${ROOT_DIR}/k8s/persistent-volume-claims" 

NAMESPACE="eaer-k8s"
BASE_IMAGE="eaer-k8s:slim"
MINIKUBE_PROFILE="${EAER_MINIKUBE_PROFILE:-domolandes}"
START_MINIKUBE=false

usage() {
    cat << 'EOF'
Usage: eaer-k8s start [options]

Start/apply EAER Kubernetes resources.

Options:
  -M, --start-minikube, --minikube  Start Minikube if profile is not running
  -h, --help, -help, help           Show this help
EOF
}

for arg in "$@"; do
    case "$arg" in
        "")
            # Ignore empty args that may be forwarded by wrappers.
            ;;
        -h|--help|-help|help)
            usage
            exit 0
            ;;
        -M|--start-minikube|--minikube)
            START_MINIKUBE=true
            ;;
        *)
            echo "Unknown option: $arg"
            usage
            exit 1
            ;;
    esac
done

shopt -s nullglob

require_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Required command not found: $cmd"
        exit 1
    fi
}

apply_yaml_dir() {
    local dir="$1"
    local description="$2"
    shift 2

    local files=("${dir}"/*.yaml)
    if [ ${#files[@]} -eq 0 ]; then
        echo "No ${description} manifests found in ${dir}."
        return
    fi

    kubectl apply -f "$dir" "$@"
}

clean_k8s_name() {
    local raw="$1"
    local cleaned
    # Keep DNS-1123 compatible characters and trim edge separators.
    cleaned="$(echo "$raw" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9.-]+/-/g; s/^[.-]+//; s/[.-]+$//')"
    if [ -z "$cleaned" ]; then
        cleaned="script-config"
    fi
    echo "$cleaned"
}

require_cmd kubectl
require_cmd minikube

# Start the cluster with Minikube if it's not already running.
if ! minikube status -p "$MINIKUBE_PROFILE" --format '{{.Host}}' 2>/dev/null | grep -q '^Running$'; then
    if [ "$START_MINIKUBE" = true ]; then
        minikube start -p "$MINIKUBE_PROFILE"
    else
        echo "Minikube profile '$MINIKUBE_PROFILE' is not running."
        echo "Start it first, or run this script with '-M' to start automatically."
        exit 1
    fi
else
    echo "Minikube profile '$MINIKUBE_PROFILE' is already running, skipping cluster start."
fi

# Create the namespace if it doesn't exist.
if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    kubectl create namespace "$NAMESPACE"
else
    echo "Namespace $NAMESPACE already exists."
fi

# Warm image cache once to reduce parallel pull failures on constrained links.
if minikube image ls -p "$MINIKUBE_PROFILE" | grep -q "$BASE_IMAGE"; then
    echo "Base image $BASE_IMAGE already present in Minikube cache."
else
    if [ "${EAER_PREPULL_IMAGE:-true}" = "true" ]; then
        pulled=false
        for attempt in 1 2 3; do
            if minikube image pull -p "$MINIKUBE_PROFILE" "$BASE_IMAGE"; then
                pulled=true
                echo "Base image $BASE_IMAGE pulled successfully."
                minikube image load -p "$MINIKUBE_PROFILE" "$BASE_IMAGE" >/dev/null
                echo "Base image $BASE_IMAGE loaded into Minikube cache."
                break
            fi
            echo "Image pull failed (attempt ${attempt}/3), retrying..."
            sleep 5
        done

        if [ "$pulled" = false ]; then
            echo "Warning: unable to pre-pull $BASE_IMAGE after retries."
        fi
    fi
fi

apply_yaml_dir "$SERVICES_DIR" "service" --namespace="$NAMESPACE"

# Create or update one ConfigMap per Python script.
python_files=("${PYTHON_SCRIPTS_DIR}"/*.py)
if [ ${#python_files[@]} -eq 0 ]; then
    echo "No Python files found in ${PYTHON_SCRIPTS_DIR}."
else
    for file in "${python_files[@]}"; do
        filename="$(basename "$file")"
        base_name="${filename%.py}"
        configmap_name="$(clean_k8s_name "$base_name")"

        kubectl create configmap "$configmap_name" --from-file="$filename=$file" --dry-run=client -o yaml \
            | kubectl apply -f - --namespace="$NAMESPACE"
    done
fi

apply_yaml_dir "$PERSISTENT_VOLUMES_DIR" "persistent volume"
apply_yaml_dir "$PERSISTENT_VOLUME_CLAIMS_DIR" "persistent volume claim" --namespace="$NAMESPACE"
apply_yaml_dir "$DEPLOYMENTS_DIR" "deployment" --namespace="$NAMESPACE"


echo "Kubernetes cluster started and services, deployments, and ConfigMaps applied successfully."