#!/bin/bash
# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

# This script print kubectl get pods in the eess-k8s namespace.
NAMESPACE="eess-k8s"

# Check if minikube is running
if ! (minikube status --format '{{.Host}}' 2>/dev/null | grep -q "Running"); then
    echo "Minikube is not running. Please start the cluster before running this script."
    exit 1
fi

# Check if the namespace exists
if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "Namespace $NAMESPACE does not exist. Please start the cluster and apply resources before running this script."
    exit 1
fi

# Watch the pods in the namespace clearing the screen before each update
while true; do
    clear
    echo "Watching pods in namespace $NAMESPACE. Press Ctrl+C to exit."
    kubectl get pods --namespace="$NAMESPACE" -o wide
    sleep 1
done