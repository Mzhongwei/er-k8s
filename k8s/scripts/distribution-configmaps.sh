#!/bin/bash

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

NAMESPACE="argo"
NORMALIZATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/normalization_distribution.py"
BERT_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/bert_distribution.py"
BERT_TRAINING_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/pipeline/bert_training.py"
CONFIG_FILE="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/config/examples/config-bert.yaml"

require_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Required command not found: $cmd"
        exit 1
    fi
}

create_or_update_configmap() {
    local configmap_name="$1"
    local key_name="$2"
    local file_path="$3"

    if [ ! -f "$file_path" ]; then
        echo "Missing file for ConfigMap $configmap_name: $file_path"
        exit 1
    fi

    kubectl create configmap "$configmap_name" \
        --from-file="${key_name}=${file_path}" \
        --dry-run=client -o yaml | \
        kubectl apply -n "$NAMESPACE" -f -
}

require_cmd kubectl

create_or_update_configmap "eaer-normalization-distribution" "normalization_distribution.py" "$NORMALIZATION_SCRIPT"
create_or_update_configmap "eaer-bert-distribution" "bert_distribution.py" "$BERT_SCRIPT"
create_or_update_configmap "eaer-bert-training" "bert_training.py" "$BERT_TRAINING_SCRIPT"
kubectl -n argo create configmap er-pipeline-config   --from-file=config.yaml=${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/config/examples/config-bert.yaml   --dry-run=client -o yaml | kubectl -n argo apply -f -