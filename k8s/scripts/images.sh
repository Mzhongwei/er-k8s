#!/usr/bin/env bash
# Build and publish the images used by the active EAER Kubernetes pipelines.

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
IMAGES_DIR="$ROOT_DIR/docker"

# Single source of truth for the Docker Hub (or other registry) repository these images
# are tagged/pushed under. compiler.py reads the same file to rewrite the `image:` field
# in every generated k8s/pipeline/exec manifest, so changing the registry/user only means
# editing this one file (or setting EAER_IMAGE_REPOSITORY for a one-off override) -- not
# hunting down every yaml that hardcodes it.
IMAGE_REPOSITORY="${EAER_IMAGE_REPOSITORY:-$(cat "$SCRIPT_DIR/image-repository.conf")}"

# These are the images used by the active batch and incremental manifests. base is built
# first because every Python component inherits from it; kafka-producer is built last
# because it inherits from kafka.
RUNTIME_IMAGE_TAGS=(
    normalization
    graph
    cgfeature
    embedding
    featureindex
    prediction
    bert
    kafka
    kafka-producer
)

DO_BUILD=false
DO_PUSH=false

usage() {
    cat << 'EOF'
Usage: erctl images [options]
Manage the Docker images used by the active EAER pipelines.

Options:
  -b, --build               Build base and all runtime images
  -p, --push                Push the exact EAER image set to Docker Hub
  -h, --help                Show this help

Options can be combined, for example: erctl images --build --push
EOF
}

docker_cmd() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo docker "$@"
    else
        docker "$@"
    fi
}

all_image_tags() {
    printf '%s\n' base
    printf '%s\n' "${RUNTIME_IMAGE_TAGS[@]}"
}

build_image() {
    local tag="$1"
    local dockerfile="$2"

    echo "Building ${IMAGE_REPOSITORY}:${tag} from ${dockerfile}..."
    docker_cmd build \
        --tag "${IMAGE_REPOSITORY}:${tag}" \
        --build-arg "IMAGE_REPOSITORY=${IMAGE_REPOSITORY}" \
        --file "$IMAGES_DIR/$dockerfile" \
        "$ROOT_DIR"
}

build_images() {
    echo "Building EAER base and runtime images..."

    build_image base Dockerfile.base

    build_image normalization Dockerfile.normalization
    build_image graph Dockerfile.graph
    build_image cgfeature Dockerfile.cgfeature
    build_image embedding Dockerfile.embedding
    build_image featureindex Dockerfile.featureindex
    build_image prediction Dockerfile.prediction
    build_image bert Dockerfile.bert
    build_image kafka Dockerfile.kafka
    build_image kafka-producer Dockerfile.kafka-producer
}

push_images() {
    local tag=""

    echo "Pushing the EAER image set to Docker Hub..."
    while IFS= read -r tag; do
        docker_cmd image inspect "${IMAGE_REPOSITORY}:${tag}" >/dev/null
        docker_cmd push "${IMAGE_REPOSITORY}:${tag}"
        echo "Pushed ${IMAGE_REPOSITORY}:${tag}"
    done < <(all_image_tags)
}

while [ $# -gt 0 ]; do
    case "$1" in
        -b|--build)
            DO_BUILD=true
            ;;
        -p|--push)
            DO_PUSH=true
            ;;
        -h|--help|-help|help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
    shift
done

if [ "$DO_BUILD" = false ] && [ "$DO_PUSH" = false ]; then
    usage
    exit 1
fi

if [ "$DO_BUILD" = true ]; then
    build_images
fi
if [ "$DO_PUSH" = true ]; then
    push_images
fi
