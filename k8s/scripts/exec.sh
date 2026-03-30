#!/bin/bash
# This script is a wrapper for an easier use of kubectl exec command.

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

NAMESPACE="eaer-k8s"

usage() {
    cat << 'EOF'
Usage: eaer-k8s exec [options] -- COMMAND [args...]
Execute a command in a running EAER pod.
Options:
  -n, --name POD_NAME           Execute command in a specific pod by name of the deployment
  -h, --help, -help, help       Show this help
EOF
}

pod_name=""

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help|-help|help)
            usage
            exit 0
            ;;
        -n|--name)
            if [ $# -lt 2 ]; then
                echo "Option $1 requires a value."
                exit 1
            fi
            pod_name="$2"
            shift
            ;;
        --name=*)
            pod_name="${1#--name=}"
            ;;
        -n=*)
            pod_name="${1#-n=}"
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
    shift
done

if [ -z "$pod_name" ]; then
    echo "Error: Pod name is required. Use -n or --name to specify the pod."
    usage
    exit 1
fi

# Get the actual pod name from the deployment selector
actual_pod=$(kubectl get pods --namespace="${NAMESPACE}" --selector="app=${pod_name}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [ -z "$actual_pod" ]; then
    echo "Error: No pod found with app=${pod_name} in namespace ${NAMESPACE}"
    exit 1
fi

kubectl exec -it --namespace="${NAMESPACE}" --container="${pod_name}" "$actual_pod" -- "$@"