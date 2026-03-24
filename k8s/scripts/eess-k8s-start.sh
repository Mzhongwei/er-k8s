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

NAMESPACE="eess-k8s"
BASE_IMAGE="python:3.12-alpine"
    
# Check if the -M flag is provided to determine if we should start the cluster in addition to applying resources.
# Start the cluster with minikube if it's not already running.
if ! minikube status --format '{{.Host}}' 2>/dev/null | grep -q "Running"; then
    if ! command -v minikube &> /dev/null; then
        echo "Minikube is not installed. Please install Minikube to start the cluster."
        exit 1
    elif [ "${1:-}" == "-M" ]; then
        minikube start
    else
        echo "Minikube is not running. Please start the cluster before running this script or use the '-M' flag to start it."
        exit 1
    fi
else
    echo "Minikube is already running, skipping cluster start."
fi

shopt -s nullglob

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

# Create the namespace if it doesn't exist.
if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    kubectl create namespace "$NAMESPACE"
else
    echo "Namespace $NAMESPACE already exists."
fi

# Warm image cache once to reduce parallel pull failures on constrained links.
if [ "${EESS_PREPULL_IMAGE:-true}" = "true" ]; then
    pulled=false
    for attempt in 1 2 3; do
        if minikube image pull "$BASE_IMAGE"; then
            pulled=true
            echo "Base image $BASE_IMAGE pulled successfully."
            break
        fi
        echo "Image pull failed (attempt ${attempt}/3), retrying..."
        sleep 5
    done

    if [ "$pulled" = false ]; then
        echo "Warning: unable to pre-pull $BASE_IMAGE after retries."
    fi
fi

# Deploy services
service_files=("${SERVICES_DIR}"/*.yaml)
if [ ${#service_files[@]} -eq 0 ]; then
    echo "No service manifests found in ${SERVICES_DIR}."
else
    for file in "${service_files[@]}"; do
        kubectl apply -f "$file" --namespace="$NAMESPACE"
    done
fi

# Create or update one ConfigMap per Python script.
python_files=("${PYTHON_SCRIPTS_DIR}"/*.py)
if [ ${#python_files[@]} -eq 0 ]; then
    echo "No Python files found in ${PYTHON_SCRIPTS_DIR}."
else
    for file in "${python_files[@]}"; do
        filename="$(basename "$file")"
        base_name="${filename%.py}"
        configmap_name="$(clean_k8s_name "$base_name")"

        kubectl create configmap "$configmap_name" --from-file="$filename=$file" --dry-run=client -o yaml | kubectl apply -f - --namespace="$NAMESPACE"
    done
fi

# Deploy deployments
deployment_files=("${DEPLOYMENTS_DIR}"/*.yaml)
if [ ${#deployment_files[@]} -eq 0 ]; then
    echo "No deployment manifests found in ${DEPLOYMENTS_DIR}."
else
    for file in "${deployment_files[@]}"; do
        kubectl apply -f "$file" --namespace="$NAMESPACE"
    done
fi


echo "Kubernetes cluster started and services, deployments, and ConfigMaps applied successfully."