#!/bin/bash

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

# This script stops Minikube and deletes the resources
NAMESPACE="eess-k8s"

shopt -s nullglob

# Raise error if minikube is not running
if ! (minikube status -p domolandes --format '{{.Host}}' 2>/dev/null | grep -q "Running"); then
    echo "Minikube is not running. Please start the cluster before running this script."
    exit 1
fi

# Delete the namespace and all resources within it. This will also delete services, deployments, and ConfigMaps.
kubectl delete namespace "$NAMESPACE" --ignore-not-found

if [ "$1" == "-M" ]; then
    # Stop the cluster with minikube if it's running.
    if minikube status -p domolandes --format '{{.Host}}' 2>/dev/null | grep -q "Running"; then
        minikube stop -p domolandes
        echo "Kubernetes cluster stopped successfully."
    else
        echo "Minikube is not running."
    fi
fi