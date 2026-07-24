#!/bin/bash

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

# -e: Immediately terminate the script if any command returns a non-zero status. 
# -u: Immediately report an error and terminate the script if an undefined variable is used. 
# -o pipefail: If any command in the pipeline fails, the entire pipeline is considered a failure.
set -euo pipefail

# Defines all top-level commands supported by the current management tool.
ACTIONS=("images" "configmaps" "dataset" "process" "pipeline" "compile" "schedule" "move" "help")

print_help() {
    cat << 'EOF'
EAER Kubernetes Cluster Manager
================================

USAGE:
  erctl COMMAND [OPTIONS]
  erctl [help|-h|--help|-help]

COMMANDS:
    images      Manage the Docker images for EAER components
    configmaps  Create batch, worker, simulator ConfigMaps, and the pipeline config ConfigMap
                from a given config file
    dataset     Sync the selected embedding or BERT dataset into the data PVC
    process     Get the PID of processes running in the Argo workflow
    pipeline    Manage the Argo pipeline workflow and its storage
    compile     Compile the node scheduling configuration
    schedule    Explain placement or run H1/H2 adaptation
    move        Move pods between nodes during execution
    help        Display this help message

OPTIONS:
    For images:
        -b --build           Build the Docker images for EAER components
        -l --load            Load the Docker images into Minikube (if using Minikube)
        -p --push            Push the Docker images to Docker Hub
        -h --help            Show help for images command

EXAMPLES:

  # Create ConfigMaps (including the pipeline config) from a given config file
  erctl configmaps code/Energy-Aware-Entity-Resolution/config/examples/config-embedding.yaml

  # Sync only the embedding dataset (embedding is the default)
  erctl dataset

  # Select BERT explicitly
  erctl configmaps code/Energy-Aware-Entity-Resolution/config/examples/config-bert.yaml
  erctl dataset bert

  # Show command-specific help
  erctl configmaps --help
  erctl dataset --help
  erctl pipeline --help
EOF
}

# Get the absolute path of the directory where the current script is located.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# $# ：The number of parameters provided by the user. If no parameters are provided, display the basic usage and exit.
if [ $# -eq 0 ]; then
    print_help
    exit 1
fi

# get the first command and analyze
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
    if [ ! -f "${SCRIPT_DIR}/${script_name}" ]; then
        echo "Error: Script ${SCRIPT_DIR}/${script_name} not found."
        exit 1
    fi
    if [[ "${script_name}" == *.py ]]; then
        python3 "${SCRIPT_DIR}/${script_name}" "$@"
    else
        bash "${SCRIPT_DIR}/${script_name}" "$@"
    fi
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
                    images_args+=("--build")
                    ;;
                -l|--load)
                    images_args+=("--load")
                    ;;
                -p|--push)
                    images_args+=("--push")
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
        if [ $# -ne 1 ]; then
            echo "Usage: erctl configmaps <config-file-path>"
            exit 1
        fi

        case "$1" in
            -h|--help|-help|help)
                cat << 'EOF'
Usage: erctl configmaps <config-file-path>

Create or update the ConfigMaps that provide the batch, worker, and simulator entry
scripts, plus the pipeline config itself -- bundled from the given file as
er-pipeline-config, so the workers run with exactly that config.
EOF
                exit 0
                ;;
            *)
                run_script "configmaps.sh" "$1"
                ;;
        esac
        ;;

    dataset)
        if [ $# -gt 1 ]; then
            echo "Usage: erctl dataset [embedding|bert]"
            exit 1
        fi

        dataset_mode="${1:-embedding}"
        case "$dataset_mode" in
            -h|--help|-help|help)
                cat << 'EOF'
Usage: erctl dataset [embedding|bert]

Create/apply the data PVC, clear its previous contents, and sync exactly one dataset family.
embedding (default): tableA.csv, tableB.csv, matches.txt
bert:                train.csv, test.csv, valid.csv
EOF
                exit 0
                ;;
            embedding|bert)
                run_script "sync-data-pvc.sh" "$dataset_mode"
                ;;
            *)
                echo "Unknown mode for dataset: $dataset_mode"
                echo "Expected 'embedding' or 'bert'."
                exit 1
                ;;
        esac
        ;;
    process)
        run_script "process.sh" "$@"
        ;;

    pipeline)
        run_script "pipeline.sh" "$@"
        ;;

    compile)
        run_script "compiler.py" "$@"
        ;;
    schedule)
        run_script "scheduling.py" "$@"
        ;;
    move)
        run_script "move.py" "$@"
        ;;
esac
