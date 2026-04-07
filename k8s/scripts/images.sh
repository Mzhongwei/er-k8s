#!/bin/bash
# This script builds the Docker images for the EAER components.
set -euo pipefail

IMAGES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/docker"

usage() {
    cat << 'EOF'
Usage: erctl images [options]
Manage the Docker images for EAER components.
Options:
  -b, --build               Build the Docker images for EAER components
  -l, --load                Load the Docker images into Minikube (if using Minikube)
  -p, --push                Push the Docker images to Docker Hub
  -h, --help, -help, help   Show this help
EOF
}

PREFIX=""
# If on linux, sudo
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    PREFIX="sudo"
fi

build() {
    echo "Building Docker images for EAER components..."
    # Build the images in the required order
    ${PREFIX} docker build -t erctl:min -f "${IMAGES_DIR}/Dockerfile.min" "${IMAGES_DIR}/.."
    ${PREFIX} docker build -t erctl:full -f "${IMAGES_DIR}/Dockerfile.full" "${IMAGES_DIR}/.."

    # Build the remaining images
    shopt -s nullglob
    dockerfiles=("${IMAGES_DIR}"/Dockerfile.*)
    for dockerfile in "${dockerfiles[@]}"; do
        image_name=$(basename "$dockerfile" | cut -d. -f2)
        if [[ "$image_name" == "min" || "$image_name" == "full" ]]; then
            continue
        fi
        ${PREFIX} docker build -t "kevinoulai/erctl:${image_name}" -f "$dockerfile" "${IMAGES_DIR}/.."
    done
}

push() {
    echo "Pushing Docker images to Docker Hub..."
    images=($(docker images --format "{{.Repository}}:{{.Tag}}" | grep "^kevinoulai/erctl:"))
    for image in "${images[@]}"; do
        ${PREFIX} docker push "$image"
        echo "Pushed $image to Docker Hub"
    done
}

load() {
    if ! command -v minikube &> /dev/null; then
        echo "Minikube is not installed. Please install Minikube to use the --load option."
        exit 1
    fi

    if ! minikube status --profile="${EAER_MINIKUBE_PROFILE:-domolandes}" &> /dev/null; then
        echo "Minikube profile '${EAER_MINIKUBE_PROFILE:-domolandes}' is not running. Please start Minikube to use the --load option."
        exit 1
    fi

    echo "Loading Docker images into Minikube..."
    images=($(docker images --format "{{.Repository}}:{{.Tag}}" | grep "^kevinoulai/erctl:"))
    for image in "${images[@]}"; do
        minikube image pull "$image" --profile="${EAER_MINIKUBE_PROFILE:-domolandes}"
        minikube image load "$image" --profile="${EAER_MINIKUBE_PROFILE:-domolandes}"
        echo "Loaded $image into Minikube"
    done
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help|-help|help)
            usage
            exit 0
            ;;
        -b|--build)
            build
            ;;
        -l|--load)
            load
            ;;
        -p|--push)
            push
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
    shift
done
