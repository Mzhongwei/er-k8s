#!/bin/bash

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

ACTIONS=("start" "stop" "restart" "metrics" "test" "logs" "help")

print_help() {
    cat << 'EOF'
EESS Kubernetes Cluster Manager
================================

USAGE:
  eess-k8s.sh COMMAND [OPTIONS]

COMMANDS:
    start       Deploy all EESS Kubernetes resources to the cluster
    stop        Remove all EESS Kubernetes resources from the cluster
    restart     Stop and then start all resources (clean restart)
    metrics     Display resource usage metrics for EESS pods
    test        Run tests against the deployed resources
    logs        Stream logs from EESS pods
    help        Display this help message

OPTIONS:
    For start, stop, restart:
        -M --minikube          Also manage the Minikube cluster itself
                               (start Minikube when deploying, stop when removing)

    For test:
        -r --resources       Test the deployed Kubernetes resources (services, deployments, ConfigMaps)
        -s --scripts         Test the scripts in pods
        -a --all             Test both deployed Kubernetes resources and scripts (default)
    
    For logs:
        -a --all             Stream logs from all EESS pods (default)
        -f --follow          Follow logs in real-time
        --name=POD_NAME      Stream logs from a specific pod by name of the deployment

    For metrics:
        -s --sort FIELD      Sort by pod|cpu|memory (default: pod)
        -o --order DIR       Sort order asc|desc (default: asc)
        -f --format TYPE     Output format table|csv|tsv (default: table)

EXAMPLES:
  # Deploy resources only
  ./eess-k8s.sh start

  # Deploy resources and start Minikube cluster
  ./eess-k8s.sh start -M

  # Remove resources and stop Minikube
  ./eess-k8s.sh stop -M

  # Clean restart with Minikube management
  ./eess-k8s.sh restart -M

  # Run tests against deployed resources
  ./eess-k8s.sh test -r

    # Show pod consumption sorted by memory (descending) as CSV
    ./eess-k8s.sh metrics -s memory -o desc -f csv

EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ $# -eq 0 ]; then
    echo "Usage: $0 [start|stop|restart|metrics|test|logs|help] [options]"
    exit 1
fi

COMMAND="$1"
shift

if [ "$COMMAND" = "help" ] || [ "$COMMAND" = "--help" ] || [ "$COMMAND" = "-h" ]; then
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
    start|stop|restart)
        manage_minikube=false
        while [ $# -gt 0 ]; do
            case "$1" in
                -M|--minikube)
                    manage_minikube=true
                    ;;
                *)
                    echo "Unknown option for $COMMAND: $1"
                    echo "Use '$0 help' for usage information."
                    exit 1
                    ;;
            esac
            shift
        done

        lifecycle_args=()
        if [ "$manage_minikube" = true ]; then
            lifecycle_args+=("-M")
        fi

        if [ "$COMMAND" = "start" ]; then
            run_script "start.sh" "${lifecycle_args[@]}"
        elif [ "$COMMAND" = "stop" ]; then
            run_script "stop.sh" "${lifecycle_args[@]}"
        else
            run_script "stop.sh" "${lifecycle_args[@]}"
            run_script "start.sh" "${lifecycle_args[@]}"
        fi
        ;;

    test)
        test_flag="-a"
        while [ $# -gt 0 ]; do
            case "$1" in
                -r|--resources)
                    test_flag="-r"
                    ;;
                -s|--scripts)
                    test_flag="-s"
                    ;;
                -a|--all)
                    test_flag="-a"
                    ;;
                *)
                    echo "Unknown option for test: $1"
                    echo "Use '$0 help' for usage information."
                    exit 1
                    ;;
            esac
            shift
        done

        run_script "test.sh" "$test_flag"
        ;;

    logs)
        logs_args=("-a")
        pod_name=""
        follow=false

        while [ $# -gt 0 ]; do
            case "$1" in
                -a|--all)
                    logs_args=("-a")
                    ;;
                -f|--follow)
                    follow=true
                    ;;
                --name=*)
                    pod_name="${1#*=}"
                    ;;
                --name)
                    if [ $# -lt 2 ]; then
                        echo "Option --name requires a value."
                        exit 1
                    fi
                    pod_name="$2"
                    shift
                    ;;
                *)
                    echo "Unknown option for logs: $1"
                    echo "Use '$0 help' for usage information."
                    exit 1
                    ;;
            esac
            shift
        done

        if [ -n "$pod_name" ]; then
            logs_args=("--name=${pod_name}")
        fi
        if [ "$follow" = true ]; then
            logs_args+=("-f")
        fi

        run_script "logs.sh" "${logs_args[@]}"
        ;;
    metrics)
        metrics_args=()
        while [ $# -gt 0 ]; do
            case "$1" in
                -h|--help|-help|help)
                    metrics_args+=("--help")
                    ;;
                -s|--sort|-o|--order|-f|--format)
                    if [ $# -lt 2 ]; then
                        echo "Option $1 requires a value."
                        exit 1
                    fi
                    metrics_args+=("$1" "$2")
                    shift
                    ;;
                *)
                    echo "Unknown option for metrics: $1"
                    echo "Use '$0 help' for usage information."
                    exit 1
                    ;;
            esac
            shift
        done

        run_script "metrics.sh" "${metrics_args[@]}"
        ;;
esac