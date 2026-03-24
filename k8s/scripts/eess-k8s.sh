#!/bin/bash
# Get the argument to determine if we should start or stop the cluster and change minikube status (-M)

START_STOP_FLAG=""
ACTIONS=("--start" "--stop" "--restart")

if [ -z "$1" ]; then
    echo "Usage: $0 [--start|--stop|--restart] [options]"
    exit 1
fi

if [ "$2" == "-M" ]; then
    START_STOP_FLAG="-M"
elif [ -n "$2" ]; then
    echo "Unknown option: $2. Use '-M' to start/stop the cluster in addition to applying/deleting resources."
    exit 1
fi

if [ "$1" == "--start" ]; then
    if [ -z "${BASH_VERSION:-}" ]; then
        exec bash "$0" "--start" "$START_STOP_FLAG"
    else
        # Call the start script
        bash "$(dirname "$0")/eess-k8s-start.sh" "$START_STOP_FLAG"
    fi
elif [ "$1" == "--stop" ]; then
    if [ -z "${BASH_VERSION:-}" ]; then
        exec bash "$0" "--stop" "$START_STOP_FLAG"
    else
        # Call the stop script
        bash "$(dirname "$0")/eess-k8s-stop.sh" "$START_STOP_FLAG"
    fi
elif [ "$1" == "--restart" ]; then
    if [ -z "${BASH_VERSION:-}" ]; then
        exec bash "$0" "--restart" "$START_STOP_FLAG"
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