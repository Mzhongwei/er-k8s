#!/bin/bash

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

# -e: Immediately terminate the script if any command returns a non-zero status. 
# -u: Immediately report an error and terminate the script if an undefined variable is used. 
# -o pipefail: If any command in the pipeline fails, the entire pipeline is considered a failure.
set -euo pipefail

# Defines all top-level commands supported by the current management tool.
ACTIONS=("images" "pipeline" "alumet" "schedule" "help")

print_help() {
    cat << 'EOF'
EAER Kubernetes Cluster Manager
================================

USAGE:
  erctl COMMAND [OPTIONS]
  erctl [help|-h|--help|-help]

COMMANDS:
    images      Manage the Docker images for EAER components
    pipeline    Manage the Argo pipeline workflow and its storage.
                  start --energy-monitor [ecofloc|alumet|ecofloc-alumet]
                                             auto-monitor energy + save under k8s/results/
                  start --results-summary  print matching, placement, and energy summary
                  start --plan-only        show placement plan without starting workloads
                  batch -d DIRECTORY       run YAML configurations sequentially
    alumet      Manage the cluster-wide Alumet collector.
                  start | stop | status | retention [DURATION]
    schedule    Explain placement or run H1/H2 adaptation
    help        Display this help message

OPTIONS:
    For images:
        -b --build           Build the Docker images for EAER components
        -p --push            Push the Docker images to Docker Hub
        -h --help            Show help for images command

EXAMPLES:

  # Show command-specific help
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
                    run_script "images/images.sh" "--help"
                    exit 0
                    ;;
                -b|--build)
                    images_args+=("--build")
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

        run_script "images/images.sh" "${images_args[@]}"
        ;;

    pipeline)
        run_script "pipeline/pipeline.sh" "$@"
        ;;

    alumet)
        action="${1:-status}"
        case "$action" in
            start|stop|status)
                [ $# -le 1 ] || { echo "Usage: erctl alumet $action"; exit 1; }
                run_script "monitoring/alumet/alumet.py" "$action"
                ;;
            retention)
                [ $# -le 2 ] || { echo "Usage: erctl alumet retention [DURATION]"; exit 1; }
                run_script "monitoring/alumet/alumet.py" retention --duration "${2:-7d}"
                ;;
            -h|--help|help)
                echo "Usage: erctl alumet [start|stop|status|retention [DURATION]]"
                ;;
            *)
                echo "Unknown Alumet action: $action"
                exit 1
                ;;
        esac
        ;;

    schedule)
        run_script "scheduling/scheduling.py" "$@"
        ;;
esac
