#!/bin/bash

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

# This script stops Minikube and deletes the resources
NAMESPACE="eaer-k8s"
MINIKUBE_PROFILE="${EAER_MINIKUBE_PROFILE:-domolandes}"
STOP_MINIKUBE=false

usage() {
    cat << 'EOF'
Usage: eaer-k8s stop [options]

Delete EAER Kubernetes namespace/resources.

Options:
  -M, --minikube                Also stop Minikube profile
  -h, --help, -help, help       Show this help
EOF
}

for arg in "$@"; do
    case "$arg" in
        "")
            ;;
        -h|--help|-help|help)
            usage
            exit 0
            ;;
        -M|--minikube)
            STOP_MINIKUBE=true
            ;;
        *)
            echo "Unknown option: $arg"
            usage
            exit 1
            ;;
    esac
done

shopt -s nullglob

# Raise error if minikube is not running
if ! (minikube status -p "$MINIKUBE_PROFILE" --format '{{.Host}}' 2>/dev/null | grep -q "Running"); then
    echo "Minikube is not running. Please start the cluster before running this script."
    exit 1
fi

# Delete the namespace and all resources within it. This will also delete services, deployments, and ConfigMaps.
kubectl delete namespace "$NAMESPACE" --ignore-not-found

if [ "$STOP_MINIKUBE" = true ]; then
    minikube stop -p "$MINIKUBE_PROFILE"
    echo "Kubernetes cluster stopped successfully."
fi