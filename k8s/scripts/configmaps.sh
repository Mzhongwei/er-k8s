#!/bin/bash

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

# Get arguments to know if we get bert or embedding config. Default bert.
CONFIG_TYPE="bert"
if [ $# -gt 0 ]; then
    case "$1" in
        embedding|bert)
            CONFIG_TYPE="$1"
            export CONFIG_TYPE
            ;;
        *)
            echo "Unknown option for configmaps: $1"
            echo "Use '$0 help' for usage information."
            exit 1
            ;;
    esac
fi
if [[ "$CONFIG_TYPE" != "bert" && "$CONFIG_TYPE" != "embedding" ]]; then
    echo "Invalid config type: $CONFIG_TYPE. Must be 'bert' or 'embedding'."
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

NAMESPACE="argo"
NORMALIZATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/normalization_distribution.py"
BERT_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/bert_distribution.py"
BERT_TRAINING_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/pipeline/bert_training.py"
CG_FEATURE_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/cg_feature_distribution.py"
FEATURE_INDEX_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/featureindex_candidate.py"
GRAPH_RANDOMWALK_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/graph_randomwalk.py"
EMBEDDING_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/embedding_calculating.py"
DECISION_EVALUATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/decision_evaluation.py"
CONFIG_FILE="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/config/examples/config-${CONFIG_TYPE}.yaml"
KAFKA_PRODUCER_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/services/producer.py"
KAFKA_CONSUMER_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/services/consumer.py"

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
create_or_update_configmap "eaer-cg-feature-distribution" "cg_feature_distribution.py" "$CG_FEATURE_SCRIPT"
create_or_update_configmap "eaer-feature-index" "featureindex_candidate.py" "$FEATURE_INDEX_SCRIPT"
create_or_update_configmap "eaer-graph-distribution" "graph_randomwalk.py" "$GRAPH_RANDOMWALK_SCRIPT"
create_or_update_configmap "eaer-embedding-calculating" "embedding_calculating.py" "$EMBEDDING_SCRIPT"
create_or_update_configmap "eaer-decision-evaluation" "decision_evaluation.py" "$DECISION_EVALUATION_SCRIPT"
create_or_update_configmap "er-pipeline-config" "config.yaml" "$CONFIG_FILE"
create_or_update_configmap "eaer-kafka-producer" "producer.py" "$KAFKA_PRODUCER_SCRIPT"
create_or_update_configmap "eaer-kafka-consumer" "consumer.py" "$KAFKA_CONSUMER_SCRIPT"