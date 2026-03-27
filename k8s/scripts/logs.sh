#!/bin/bash
# This script streams logs from EAER pods. It can stream logs from all pods or a specific pod by name.
# re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

NAMESPACE="eaer-k8s"

usage() {
    cat << 'EOF'
Usage: eaer-k8s logs [option]

Stream logs from EAER pods.

Options:
  -a, --all                     Stream logs from all EAER pods (default)
  -f, --follow                  Follow logs in real-time
  -n, --name=POD_NAME           Stream logs from pods matching app=POD_NAME
  -n POD_NAME, --name POD_NAME  Same as --name=POD_NAME
  -h, --help, -help, help       Show this help
EOF
}

mode="all"
pod_name=""
follow=false

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help|-help|help)
            usage
            exit 0
            ;;
        -a|--all)
            mode="all"
            ;;
        -f|--follow)
            follow=true
            ;;
        -n=*)
            mode="name"
            pod_name="${1#-n=}"
            ;;
        --name=*)
            mode="name"
            pod_name="${1#--name=}"
            ;;
        -n)
            if [ $# -lt 2 ]; then
                echo "Option -n requires a value."
                exit 1
            fi
            mode="name"
            pod_name="$2"
            shift
            ;;
        --name)
            if [ $# -lt 2 ]; then
                echo "Option --name requires a value."
                exit 1
            fi
            mode="name"
            pod_name="$2"
            shift
            ;;
        *)
            echo "Invalid option: $1"
            usage
            exit 1
            ;;
    esac
    shift
done

follow_arg=()
if [ "$follow" = true ]; then
    follow_arg+=("--follow")
fi

if [ "$mode" = "all" ]; then
    # Stream logs from all pods in the namespace
    kubectl logs -n "$NAMESPACE" --selector=app=eaer "${follow_arg[@]}"
else
    kubectl logs -n "$NAMESPACE" --selector="app=${pod_name}" "${follow_arg[@]}"
fi