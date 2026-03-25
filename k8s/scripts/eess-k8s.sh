#!/bin/bash
# Get the argument to determine if we should start or stop the cluster and change minikube status (-M)

START_STOP_FLAG=""
TEST_FLAG="--all"
ACTIONS=("start" "stop" "restart" "test" "help")

if [ -z "$1" ]; then
    echo "Usage: $0 [start|stop|restart|test|help] [options]"
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
    test        Run tests against the deployed resources
    help        Display this help message

OPTIONS:
    For start, stop, restart:
        -M --minikube          Also manage the Minikube cluster itself
                               (start Minikube when deploying, stop when removing)

    For test:
        -r --resources       Test the deployed Kubernetes resources (services, deployments, ConfigMaps)
        -s --scripts         Test the scripts in pods
        -a --all             Test both deployed Kubernetes resources and scripts (default)

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

EOF
    exit 0
fi

if [ "$2" == "-M" ] || [ "$2" == "--minikube" ] && [[ "$1" == "start" ] || [ "$1" == "stop" ] || [ "$1" == "restart" ]]; then
    START_STOP_FLAG="-M"
elif [ "$2" == "-r" ] || [ "$2" == "--resources" ] && [ "$1" == "test" ]; then
    TEST_FLAG="-r"
elif [ "$2" == "-s" ] || [ "$2" == "--scripts" ] && [ "$1" == "test" ]; then
    TEST_FLAG="-s"
elif [ "$2" == "-a" ] || [ "$2" == "--all" ] && [ "$1" == "test" ]; then
    TEST_FLAG="-a"
elif [ -n "$2" ]; then
    echo "Unknown option: $2. Use help for usage information."
    exit 1
fi

if [ "$1" == "start" ]; then
    if [ -z "${BASH_VERSION:-}" ]; then
        exec bash "$0" "start" "$START_STOP_FLAG"
    else
        # Call the start script
        bash "$(dirname "$0")/start.sh" "$START_STOP_FLAG"
    fi
elif [ "$1" == "stop" ]; then
    if [ -z "${BASH_VERSION:-}" ]; then
        exec bash "$0" "stop" "$START_STOP_FLAG"
    else
        # Call the stop script
        bash "$(dirname "$0")/stop.sh" "$START_STOP_FLAG"
    fi
elif [ "$1" == "restart" ]; then
    if [ -z "${BASH_VERSION:-}" ]; then
        exec bash "$0" "restart" "$START_STOP_FLAG"
    else
        # Call the stop script
        bash "$(dirname "$0")/stop.sh" "$START_STOP_FLAG"
        # Call the start script
        bash "$(dirname "$0")/start.sh" "$START_STOP_FLAG"
    fi
elif [ "$1" == "test" ]; then
    if [ -z "${BASH_VERSION:-}" ]; then
        exec bash "$0" "test" "$TEST_FLAG"
    else
        # Call the test script
        bash "$(dirname "$0")/test.sh" "$TEST_FLAG"
    fi
elif [[ ! " ${ACTIONS[*]} " == *" $1 "* ]]; then
    echo "Invalid argument: $1. Use one of the following: ${ACTIONS[*]}."
    exit 1
fi