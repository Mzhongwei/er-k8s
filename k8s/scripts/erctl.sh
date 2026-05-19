#!/bin/bash

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

ACTIONS=("images" "configmaps" "dataset" "fetch-report" "process" "pipeline" "help")

print_help() {
    cat << 'EOF'
EAER Kubernetes Cluster Manager
================================

USAGE:
  erctl COMMAND [OPTIONS]
  erctl [help|-h|--help|-help]

COMMANDS:
    images      Manage the Docker images for EAER components
    configmaps  Create or update the distribution ConfigMaps
    dataset     Sync Data_example/bert files into the Argo PVC
    process     Get the PID of processes running in the Argo workflow
    pipeline    Manage the Argo pipeline workflow and its storage
    help        Display this help message

OPTIONS:
    For images:
        -b --build           Build the Docker images for EAER components
        -l --load            Load the Docker images into Minikube (if using Minikube)
        -p --push            Push the Docker images to Docker Hub
        -h --help            Show help for images command

EXAMPLES:

  # Create or update distribution ConfigMaps without starting workloads
  erctl configmaps

  # Sync local BERT CSV fixtures to the PVC used by Argo
  erctl dataset

  # Show command-specific help
  erctl configmaps --help
  erctl dataset --help
    erctl pipeline --help
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ $# -eq 0 ]; then
    echo "Usage: $0 [images|configmaps|dataset|fetch-report|process|pipeline|help] [options]"
    exit 1
fi

COMMAND="$1"
shift

if [ "$COMMAND" = "help" ] || [ "$COMMAND" = "--help" ] || [ "$COMMAND" = "-h" ] || [ "$COMMAND" = "-help" ]; then
    print_help
    exit 0
fi

if [[ ! " ${ACTIONS[*]} " == *" $COMMAND "* ]]; then
    echo "Invalid command: $COMMAND. Use one of the following: ${ACTIONS[*]}."
    exit 1
fi

run_script() {
    local script_name="$1"
    shift
    bash "${SCRIPT_DIR}/${script_name}" "$@"
}

case "$COMMAND" in
    images)
        images_args=()
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help|-help|help)
                    run_script "images.sh" "--help"
                    exit 0
                    ;;
                -b|--build)
                    images_args=("--build")
                    ;;
                -l|--load)
                    images_args=("--load")
                    ;;
                -p|--push)
                    images_args=("--push")
                    ;;
                *)
                    echo "Unknown option for images: $1"
                    echo "Use '$0 help' for usage information."
                    exit 1
                    ;;
            esac
            shift
        done

        run_script "images.sh" "${images_args[@]}"
        ;;

    configmaps)
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help|-help|help)
                    cat << 'EOF'
Usage: erctl configmaps

Create or update the ConfigMaps that provide the distribution Python scripts.
EOF
                    exit 0
                    ;;
                embedding|bert)
                    run_script "configmaps.sh" "$1"
                    exit 0
                    ;;
                *)
                    echo "Unknown option for configmaps: $1"
                    echo "Use '$0 help' for usage information."
                    exit 1
                    ;;
            esac
            shift
        done

        run_script "configmaps.sh"
        ;;

    dataset)
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help|-help|help)
                    cat << 'EOF'
Usage: erctl dataset

Create/apply the Argo PVC and sync local Data_example/bert CSV files into it.
EOF
                    exit 0
                    ;;
                *)
                    echo "Unknown option for dataset: $1"
                    echo "Use '$0 help' for usage information."
                    exit 1
                    ;;
            esac
        done

        run_script "sync-bert-data-pvc.sh"
        ;;
    fetch-report)
        # forward all args to the helper script
        run_script "fetch-codecarbon.sh" "$@"
        ;;

    process)
        run_script "process.sh" "$@"
        ;;

    pipeline)
        run_script "pipeline.sh" "$@"
        ;;

    *)
        echo "Invalid command: $COMMAND. Use one of the following: ${ACTIONS[*]}."
        exit 1
        ;;
esac
