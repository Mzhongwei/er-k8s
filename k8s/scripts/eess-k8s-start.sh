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
    
# Check if the -M flag is provided to determine if we should start the cluster in addition to applying resources.
if [ "$1" == "-M" ]; then
    # Start the cluster with minikube if it's not already running.
    if ! minikube status --format '{{.Host}}' 2>/dev/null | grep -q "Running"; then
        minikube start
        echo "Kubernetes cluster started successfully."
    else
        echo "Minikube is already running."
    fi
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
    echo "Namespace $NAMESPACE created successfully."
else
    echo "Namespace $NAMESPACE already exists."
fi

# Deploy services
service_files=("${SERVICES_DIR}"/*.yaml)
if [ ${#service_files[@]} -eq 0 ]; then
    echo "No service manifests found in ${SERVICES_DIR}."
else
    for file in "${service_files[@]}"; do
        kubectl apply -f "$file" --namespace="$NAMESPACE"
        service_name="$(grep -m1 -E '^[[:space:]]*name:' "$file" | awk '{print $2}' || true)"
        echo "Service ${service_name:-$(basename "$file")} deployed successfully."
    done
fi

# Deploy deployments
deployment_files=("${DEPLOYMENTS_DIR}"/*.yaml)
if [ ${#deployment_files[@]} -eq 0 ]; then
    echo "No deployment manifests found in ${DEPLOYMENTS_DIR}."
else
    for file in "${deployment_files[@]}"; do
        kubectl apply -f "$file" --namespace="$NAMESPACE"
        deployment_name="$(grep -m1 -E '^[[:space:]]*name:' "$file" | awk '{print $2}' || true)"
        echo "Deployment ${deployment_name:-$(basename "$file")} deployed successfully."
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
        echo "ConfigMap ${configmap_name} created/updated successfully."
    done
fi

echo "Kubernetes cluster started and services, deployments, and ConfigMaps applied successfully."