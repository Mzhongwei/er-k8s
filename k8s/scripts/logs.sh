#!/bin/bash
# This script streams logs from EESS pods. It can stream logs from all pods or a specific pod by name.
# re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

NAMESPACE="eess-k8s"

if [ "$1" == "-a" ] || [ "$1" == "--all" ]; then
    # Stream logs from all pods in the namespace
    kubectl logs -n "$NAMESPACE" --selector=app=eess --follow
elif [[ "$1" == "--name="* ]]; then
    # Extract pod name from the argument
    POD_NAME="${1#--name=}"
    kubectl logs -n "$NAMESPACE" --selector=app=$POD_NAME ${2:-}
else
    echo "Invalid option: $1. Use '-a' to stream logs from all pods or '--name=POD_NAME' to stream logs from a specific pod."
    exit 1
fi