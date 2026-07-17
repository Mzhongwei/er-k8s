#!/bin/bash

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

# Path to the pipeline config YAML to bundle as er-pipeline-config. No mode/family
# selector here -- the caller (erctl pipeline) already derived everything it needs from
# this same file's own `mode:` field before invoking us.
if [ $# -ne 1 ]; then
    echo "Usage: erctl configmaps <config-file-path>"
    exit 1
fi
CONFIG_FILE="$1"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file not found: $CONFIG_FILE"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

NAMESPACE="argo"

# --- legacy: distributions/ (superseded by entries/worker + entries/batch). The
# corresponding templates in batch/pipeline.yaml are commented out too, so these
# ConfigMaps are disabled here as well; kept as comments for migration reference. ---
# NORMALIZATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/normalization_distribution.py"
# BERT_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/bert_distribution.py"
BERT_TRAINING_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/pipeline/bert_training.py"
# CG_FEATURE_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/cg_feature_distribution.py"
# FEATURE_INDEX_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/featureindex_candidate.py"
# GRAPH_RANDOMWALK_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/graph_randomwalk.py"
# EMBEDDING_TRAINING_ENTRY_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/embedding_training_entry.py"
# CALCULATING_SIMILARITY_ENTRY_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/calculating_similarity_entry.py"
# DECISION_EVALUATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/decision_evaluation.py"
# EMBEDDING_COMMON_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/embedding_common.py"
# was state_io/embedding_state.py; that module moved to distributions/embedding_state.py
# EMBEDDING_STATE_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/distributions/embedding_state.py"

# --- entries/worker: incremental pipeline step scripts (one script per step, no
# runtime --mode/--function branching) ---
WORKER_NORMALIZATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/worker/normalization_embedding.py"
WORKER_GRAPH_CONSTRUCTION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/worker/graph_construction.py"
WORKER_RANDOM_WALK_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/worker/random_walk.py"
WORKER_CG_FEATURE_EXTRACTION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/worker/cg_feature_extraction.py"
WORKER_EMBEDDING_TRAINING_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/worker/embedding_training.py"
WORKER_CANDIDATE_ENUMERATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/worker/candidate_enumeration.py"
WORKER_CALCULATING_SIMILARITY_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/worker/calculating_similarity.py"
WORKER_DECISION_MAKING_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/worker/decision_making.py"
WORKER_EVALUATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/worker/evaluation.py"

# --- entries/batch: embedding-training-dag steps (EmbTrai-*) and bert training/evaluation
# steps (BertTrai-*/BertEva-*). Batch embedding-prediction (EmbPred-*) is out of scope here. ---
BATCH_EMBTRAI_NORMALIZATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/batch/EmbTrai-normalization.py"
BATCH_EMBTRAI_GRAPH_CONSTRUCTION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/batch/EmbTrai-graph_construction.py"
BATCH_EMBTRAI_RANDOM_WALK_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/batch/EmbTrai-random_walk.py"
BATCH_EMBTRAI_CG_FEATURE_EXTRACTION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/batch/EmbTrai-cg_feature_extraction.py"
BATCH_EMBTRAI_EMBEDDING_TRAINING_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/batch/EmbTrai-embedding_training.py"
BATCH_EMBTRAI_FEATURE_INDEX_CONSTRUCTION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/batch/EmbTrai-feature_index_construction.py"
BATCH_BERTTRAI_NORMALIZATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/batch/BertTrai-normalization.py"
BATCH_BERTTRAI_TRAINING_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/batch/BertTrai_training.py"
BATCH_BERTEVA_NORMALIZATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/batch/BertEva-normalization.py"
BATCH_BERTEVA_B_EVALUATION_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/batch/BertEva-b_evaluation.py"

KAFKA_PRODUCER_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/simulators/producer.py"
KAFKA_CONSUMER_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/entries/simulators/consumer.py"
PIPELINE_IO_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/utils/pipeline_io.py"
INCREMENTAL_INIT_SCRIPT="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/incremental_init.sh"
VERSION_NAME="${EAER_PIPELINE_VERSION_NAME:-}"
TEMP_FILES=()

cleanup_temp_files() {
    local temp_file=""

    for temp_file in "${TEMP_FILES[@]:-}"; do
        [ -n "$temp_file" ] && rm -f "$temp_file" 2>/dev/null || true
    done
}

trap cleanup_temp_files EXIT

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

prepare_config_file() {
    local source_file="$1"
    local version_name="$2"
    local temp_file=""

    temp_file="$(mktemp)"

    if grep -qE '^[[:space:]]*(version_name|versionName)[[:space:]]*:' "$source_file"; then
        awk -v version_name="$version_name" '
            BEGIN { replaced = 0 }
            /^[[:space:]]*(version_name|versionName)[[:space:]]*:/ && !replaced {
                sub(/:.*/, ": \"" version_name "\"")
                replaced = 1
            }
            { print }
            END {
                if (!replaced) {
                    print "version_name: \"" version_name "\""
                }
            }
        ' "$source_file" > "$temp_file"
    elif grep -qF '__VERSION_NAME__' "$source_file"; then
        sed "s/__VERSION_NAME__/${version_name//\/\\}/g" "$source_file" > "$temp_file"
    else
        cat "$source_file" > "$temp_file"
        printf '\nversion_name: "%s"\n' "$version_name" >> "$temp_file"
    fi

    TEMP_FILES+=("$temp_file")
    printf '%s\n' "$temp_file"
}

require_cmd kubectl

CONFIG_FILE_TO_USE="$CONFIG_FILE"
if [ -n "$VERSION_NAME" ]; then
    CONFIG_FILE_TO_USE="$(prepare_config_file "$CONFIG_FILE" "$VERSION_NAME")"
fi

# --- legacy: distributions/ (disabled, see variable block above) ---
# create_or_update_configmap "eaer-normalization-distribution" "normalization_distribution.py" "$NORMALIZATION_SCRIPT"
# create_or_update_configmap "eaer-bert-distribution" "bert_distribution.py" "$BERT_SCRIPT"
create_or_update_configmap "eaer-bert-training" "bert_training.py" "$BERT_TRAINING_SCRIPT"
# create_or_update_configmap "eaer-cg-feature-distribution" "cg_feature_distribution.py" "$CG_FEATURE_SCRIPT"
# create_or_update_configmap "eaer-feature-index" "featureindex_candidate.py" "$FEATURE_INDEX_SCRIPT"
# create_or_update_configmap "eaer-graph-distribution" "graph_randomwalk.py" "$GRAPH_RANDOMWALK_SCRIPT"
# create_or_update_configmap "eaer-embedding-training-entry" "embedding_training_entry.py" "$EMBEDDING_TRAINING_ENTRY_SCRIPT"
# create_or_update_configmap "eaer-calculating-similarity-entry" "calculating_similarity_entry.py" "$CALCULATING_SIMILARITY_ENTRY_SCRIPT"
# create_or_update_configmap "eaer-decision-evaluation" "decision_evaluation.py" "$DECISION_EVALUATION_SCRIPT"
# create_or_update_configmap "eaer-embedding-common" "embedding_common.py" "$EMBEDDING_COMMON_SCRIPT"
# create_or_update_configmap "eaer-embedding-state" "embedding_state.py" "$EMBEDDING_STATE_SCRIPT"

# --- entries/worker ---
create_or_update_configmap "eaer-worker-normalization" "normalization_embedding.py" "$WORKER_NORMALIZATION_SCRIPT"
create_or_update_configmap "eaer-worker-graph-construction" "graph_construction.py" "$WORKER_GRAPH_CONSTRUCTION_SCRIPT"
create_or_update_configmap "eaer-worker-random-walk" "random_walk.py" "$WORKER_RANDOM_WALK_SCRIPT"
create_or_update_configmap "eaer-worker-cg-feature-extraction" "cg_feature_extraction.py" "$WORKER_CG_FEATURE_EXTRACTION_SCRIPT"
create_or_update_configmap "eaer-worker-embedding-training" "embedding_training.py" "$WORKER_EMBEDDING_TRAINING_SCRIPT"
create_or_update_configmap "eaer-worker-candidate-enumeration" "candidate_enumeration.py" "$WORKER_CANDIDATE_ENUMERATION_SCRIPT"
create_or_update_configmap "eaer-worker-calculating-similarity" "calculating_similarity.py" "$WORKER_CALCULATING_SIMILARITY_SCRIPT"
create_or_update_configmap "eaer-worker-decision-making" "decision_making.py" "$WORKER_DECISION_MAKING_SCRIPT"
create_or_update_configmap "eaer-worker-evaluation" "evaluation.py" "$WORKER_EVALUATION_SCRIPT"

# --- entries/batch ---
create_or_update_configmap "eaer-batch-embtrai-normalization" "EmbTrai-normalization.py" "$BATCH_EMBTRAI_NORMALIZATION_SCRIPT"
create_or_update_configmap "eaer-batch-embtrai-graph-construction" "EmbTrai-graph_construction.py" "$BATCH_EMBTRAI_GRAPH_CONSTRUCTION_SCRIPT"
create_or_update_configmap "eaer-batch-embtrai-random-walk" "EmbTrai-random_walk.py" "$BATCH_EMBTRAI_RANDOM_WALK_SCRIPT"
create_or_update_configmap "eaer-batch-embtrai-cg-feature-extraction" "EmbTrai-cg_feature_extraction.py" "$BATCH_EMBTRAI_CG_FEATURE_EXTRACTION_SCRIPT"
create_or_update_configmap "eaer-batch-embtrai-embedding-training" "EmbTrai-embedding_training.py" "$BATCH_EMBTRAI_EMBEDDING_TRAINING_SCRIPT"
create_or_update_configmap "eaer-batch-embtrai-feature-index-construction" "EmbTrai-feature_index_construction.py" "$BATCH_EMBTRAI_FEATURE_INDEX_CONSTRUCTION_SCRIPT"
create_or_update_configmap "eaer-batch-berttrai-normalization" "BertTrai-normalization.py" "$BATCH_BERTTRAI_NORMALIZATION_SCRIPT"
create_or_update_configmap "eaer-batch-berttrai-training" "BertTrai_training.py" "$BATCH_BERTTRAI_TRAINING_SCRIPT"
create_or_update_configmap "eaer-batch-berteva-normalization" "BertEva-normalization.py" "$BATCH_BERTEVA_NORMALIZATION_SCRIPT"
create_or_update_configmap "eaer-batch-berteva-b-evaluation" "BertEva-b_evaluation.py" "$BATCH_BERTEVA_B_EVALUATION_SCRIPT"

create_or_update_configmap "er-pipeline-config" "config.yaml" "$CONFIG_FILE_TO_USE"
create_or_update_configmap "eaer-kafka-producer" "producer.py" "$KAFKA_PRODUCER_SCRIPT"
create_or_update_configmap "eaer-kafka-consumer" "consumer.py" "$KAFKA_CONSUMER_SCRIPT"
create_or_update_configmap "eaer-pipeline-io" "pipeline_io.py" "$PIPELINE_IO_SCRIPT"
create_or_update_configmap "eaer-incremental-init" "incremental_init.sh" "$INCREMENTAL_INIT_SCRIPT"
