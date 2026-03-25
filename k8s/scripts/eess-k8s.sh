#!/bin/bash
# Get the argument to determine if we should start or stop the cluster and change minikube status (-M)

START_STOP_FLAG=""
ACTIONS=("start" "stop" "restart" "help")

if [ -z "$1" ]; then
    echo "Usage: $0 [start|stop|restart|help] [options]"
    exit 1
fi

if [ "$1" == "help" ]; then
    cat << 'EOF'
EESS Kubernetes Cluster Manager
================================

USAGE:
  eess-k8s.sh COMMAND [OPTIONS]

COMMANDS:
  start       Deploy all EESS Kubernetes resources to the cluster
  stop        Remove all EESS Kubernetes resources from the cluster
  restart     Stop and then start all resources (clean restart)
  help        Display this help message

OPTIONS:
  -M          Also manage the Minikube cluster itself
              (start Minikube when deploying, stop when removing)

EXAMPLES:
  # Deploy resources only
  ./eess-k8s.sh start

  # Deploy resources and start Minikube cluster
  ./eess-k8s.sh start -M

  # Remove resources and stop Minikube
  ./eess-k8s.sh stop -M

  # Clean restart with Minikube management
  ./eess-k8s.sh restart -M

EOF
    exit 0
fi

if [ "$2" == "-M" ]; then
    START_STOP_FLAG="-M"
elif [ -n "$2" ]; then
    echo "Unknown option: $2. Use '-M' to start/stop the cluster in addition to applying/deleting resources."
    exit 1
fi

if [ "$1" == "start" ]; then
    if [ -z "${BASH_VERSION:-}" ]; then
        exec bash "$0" "start" "$START_STOP_FLAG"
    else
        # Call the start script
        bash "$(dirname "$0")/eess-k8s-start.sh" "$START_STOP_FLAG"
    fi
elif [ "$1" == "stop" ]; then
    if [ -z "${BASH_VERSION:-}" ]; then
        exec bash "$0" "stop" "$START_STOP_FLAG"
    else
        # Call the stop script
        bash "$(dirname "$0")/eess-k8s-stop.sh" "$START_STOP_FLAG"
    fi
elif [ "$1" == "restart" ]; then
    if [ -z "${BASH_VERSION:-}" ]; then
        exec bash "$0" "restart" "$START_STOP_FLAG"
    else
        # Call the stop script
        bash "$(dirname "$0")/eess-k8s-stop.sh" "$START_STOP_FLAG"
        # Call the start script
        bash "$(dirname "$0")/eess-k8s-start.sh" "$START_STOP_FLAG"
    fi
elif [[ ! " ${ACTIONS[*]} " == *" $1 "* ]]; then
    echo "Invalid argument: $1. Use one of the following: ${ACTIONS[*]}."
    exit 1
fi