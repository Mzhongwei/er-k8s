#!/bin/bash
# This script tests the deployed resources by running a series of checks and validations.

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail
shopt -s nullglob

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SERVICES_DIR="${ROOT_DIR}/k8s/services"
DEPLOYMENTS_DIR="${ROOT_DIR}/k8s/deployments"
PYTHON_SCRIPTS_DIR="${ROOT_DIR}/code/python_files"
PERSISTENT_VOLUMES_DIR="${ROOT_DIR}/k8s/persistent-volumes"
PERSISTENT_VOLUME_CLAIMS_DIR="${ROOT_DIR}/k8s/persistent-volume-claims"

NAMESPACE="eaer-k8s"
BASE_IMAGE="eaer-k8s:slim"
ARG1="${1:-}"

usage() {
    cat << 'EOF'
Usage: eaer-k8s test [option]

Run EAER tests.

Options:
  -r, --resources              Test Kubernetes manifests/resources
  -s, --scripts                Test script behavior via pod logs
  -a, --all                    Run both resource and script tests (default)
  -h, --help, -help, help      Show this help
EOF
}

if [ -z "$ARG1" ]; then
    ARG1="-a"
fi

case "$ARG1" in
    -h|--help|-help|help)
        usage
        exit 0
        ;;
    -r|--resources|-s|--scripts|-a|--all)
        ;;
    *)
        echo "Unknown option: $ARG1"
        usage
        exit 1
        ;;
esac

RED='\033[1;33;41m'
BLUE='\033[1;33;44m'
GREEN='\033[1;97;42m'
RESET='\033[0m'

print_error() {
    printf "%b\n" "${RED}$1${RESET}"
}

print_result() {
    printf "%b\n" "${BLUE}$1${RESET}"
}

print_success() {
    printf "%b\n" "${GREEN}$1${RESET}"
}

expected_log_lines_for_app() {
    case "$1" in
        normalization)
            cat <<'EOF'
Input : source_data_simulated
Processed data sent to CG_FEATURE_EXTRACTION_SERVICE
Processed data sent to GRAPH_CONSTRUCTION_SERVICE
Processed data sent to BERT_INFERENCE_SERVICE
Processed data sent to BERT_TRAINING_SERVICE
EOF
            ;;
        cg-feature-extraction)
            cat <<'EOF'
Received data: processed_data_simulated
CG feature sent to FEATURE_INDEX_CONSTRUCTION_SERVICE
CG feature sent to CANDIDATE_ENUMERATION_SERVICE
EOF
            ;;
        graph-construction)
            cat <<'EOF'
Received data: processed_data_simulated
Representation graph written and path sent to RANDOM_WALK_SERVICE
EOF
            ;;
        bert-inference)
            cat <<'EOF'
Received data: /pipeline/bert/bert_model.txt
BERT model file content: BERT_model_content
Prediction matching completed
EOF
            ;;
        bert-training)
            cat <<'EOF'
Received data: processed_data_simulated
BERT model sent to BERT_INFERENCE_SERVICE
EOF
            ;;
        candidate-enumeration)
            cat <<'EOF'
Received data: /pipeline/cg_features_index/cg_features_index.txt
CG features index file content: cg_features_index_content
Received data: cg_feature_simulated
Candidate pairs sent to CALCULATING_SIMILARITY_SERVICE
Candidate pairs sent to BERT_INFERENCE_SERVICE
EOF
            ;;
        feature-index-construction)
            cat <<'EOF'
Received data: cg_feature_simulated
CG features index sent to CANDIDATE_ENUMERATION_SERVICE
EOF
            ;;
        random-walk)
            cat <<'EOF'
Received data: /pipeline/graph/representation_graph.txt
Graph file content: representation_graph_content
Sequences sent to EMBEDDING_TRAINING_SERVICE
EOF
            ;;
        embedding-training)
            cat <<'EOF'
Received data: sequences_simulated
Embedding model sent to CALCULATING_SIMILARITY_SERVICE
EOF
            ;;
        calculating-similarity)
            cat <<'EOF'
Received data: candidate_pairs_simulated
Received data: /pipeline/embedding/embedding_model.txt
Embedding model file content: embedding_model_content
Similarity data sent to DECISION_MAKING_SERVICE
EOF
            ;;
        decision-making)
            cat <<'EOF'
Received data: similarity_data_simulated
Prediction matching completed
EOF
            ;;
        *)
            ;;
    esac
}

run_script_log_unit_test() {
    local file="$1"
    local filename base_name app_name logs failure_hint expected_lines missing_lines line

    filename="$(basename "$file")"
    base_name="${filename%.py}"
    app_name="$(clean_k8s_name "$base_name")"

    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    if ! kubectl -n "$NAMESPACE" get deployment "$app_name" >/dev/null 2>&1; then
        print_error "Then FAIL: deployment ${app_name} not found for ${filename}."
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return
    fi

    # if ! kubectl -n "$NAMESPACE" wait --for=condition=Ready pod -l "app=$app_name" --timeout=5s >/dev/null 2>&1; then
    #     failure_hint="$(kubectl -n "$NAMESPACE" get pods -l "app=$app_name" -o custom-columns='WAITING_REASON:.status.containerStatuses[*].state.waiting.reason,PHASE:.status.phase' --no-headers 2>/dev/null || echo "Not Found")"
    #     print_error "Pod readiness check failed for ${filename}. Status: ${failure_hint:-Not Found}"
    #     FAILED_TESTS=$((FAILED_TESTS + 1))
    #     return
    # fi

    logs="$(kubectl -n "$NAMESPACE" logs -l "app=$app_name" --all-containers=true --tail=400 2>&1 || true)"

    expected_lines="$(expected_log_lines_for_app "$app_name")"
    if [ -z "${expected_lines//[[:space:]]/}" ]; then
        print_error "No expected log pattern configured for app=${app_name}."
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return
    fi

    if [ -z "${logs//[[:space:]]/}" ]; then
        print_error "Empty logs for ${filename} (app ${app_name})."
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return
    fi

    if echo "$logs" | grep -Eiq 'traceback|exception|fatal|crashloopbackoff|segmentation fault'; then
        failure_hint="$(echo "$logs" | grep -Eim1 'traceback|exception|fatal|crashloopbackoff|segmentation fault')"
        print_error "Error signature found for ${filename}: ${failure_hint}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        return
    fi

    missing_lines=""
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        if ! echo "$logs" | grep -Fq "$line"; then
            if [ -n "$missing_lines" ]; then
                missing_lines="${missing_lines}; $line"
            else
                missing_lines="$line"
            fi
        fi
    done <<< "$expected_lines"

    if [ -n "$missing_lines" ]; then
        print_error "Expected log lines missing for ${filename}: ${missing_lines}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
    else
        print_success "Log unit test passed for ${filename}."
        SUCCESS_TESTS=$((SUCCESS_TESTS + 1))
    fi
}

clean_k8s_name() {
    local raw="$1"
    local cleaned
    # Keep DNS-1123 compatible characters and trim edge separators.
    cleaned="$(echo "$raw" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9.-]+/-/g; s/^[.-]+//; s/[.-]+$//')"
    if [ -z "$cleaned" ]; then
        cleaned="script-config"
    fi
    echo "$cleaned"
}

FAILED_TESTS=0
SUCCESS_TESTS=0
TOTAL_TESTS=0

TOTAL_TESTS=$((TOTAL_TESTS + 1))
if minikube status -p domolandes --format '{{.Host}}' 2>/dev/null | grep -q "Running"; then
    print_success "Minikube is running"
    SUCCESS_TESTS=$((SUCCESS_TESTS + 1))
else
    print_error "Minikube is not running. Please start the cluster before running this script."
    FAILED_TESTS=$((FAILED_TESTS + 1))
    print_result "Tests passed: $SUCCESS_TESTS/$TOTAL_TESTS"
    exit 1
fi

TOTAL_TESTS=$((TOTAL_TESTS + 1))
if kubectl get namespace eaer-k8s >/dev/null 2>&1; then
    print_success "Namespace eaer-k8s exists."
    SUCCESS_TESTS=$((SUCCESS_TESTS + 1))
else
    print_error "Namespace eaer-k8s does not exist. Please start the cluster and apply resources before running tests."
    FAILED_TESTS=$((FAILED_TESTS + 1))
    print_result "Tests passed: $SUCCESS_TESTS/$TOTAL_TESTS"
    exit 1
fi

if [ "$ARG1" == "-r" ] || [ "$ARG1" == "--resources" ] || [ "$ARG1" == "-a" ] || [ "$ARG1" == "--all" ]; then
    echo "Running resource tests"
    # Test services
    for service in "$SERVICES_DIR"/*.yaml; do
        TOTAL_TESTS=$((TOTAL_TESTS + 1))
        if kubectl -n "$NAMESPACE" apply --dry-run=client -f "$service" >/dev/null 2>&1; then
            print_success "Service $(basename "$service") is valid."
            SUCCESS_TESTS=$((SUCCESS_TESTS + 1))
        else
            print_error "Service $(basename "$service") is invalid."
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    done

    # Test persistent volumes
    for pv in "$PERSISTENT_VOLUMES_DIR"/*.yaml; do
        TOTAL_TESTS=$((TOTAL_TESTS + 1))
        if kubectl apply --dry-run=client -f "$pv" >/dev/null 2>&1; then
            print_success "PersistentVolume $(basename "$pv") is valid."
            SUCCESS_TESTS=$((SUCCESS_TESTS + 1))
        else
            print_error "PersistentVolume $(basename "$pv") is invalid."
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    done

    # Test persistent volume claims
    for pvc in "$PERSISTENT_VOLUME_CLAIMS_DIR"/*.yaml; do
        TOTAL_TESTS=$((TOTAL_TESTS + 1))
        if kubectl -n "$NAMESPACE" apply --dry-run=client -f "$pvc" >/dev/null 2>&1; then
            print_success "PersistentVolumeClaim $(basename "$pvc") is valid."
            SUCCESS_TESTS=$((SUCCESS_TESTS + 1))
        else
            print_error "PersistentVolumeClaim $(basename "$pvc") is invalid."
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    done

    # Test deployments
    for deployment in "$DEPLOYMENTS_DIR"/*.yaml; do
        TOTAL_TESTS=$((TOTAL_TESTS + 1))
        if kubectl -n "$NAMESPACE" apply --dry-run=client -f "$deployment" >/dev/null 2>&1; then
            print_success "Deployment $(basename "$deployment") is valid."
            SUCCESS_TESTS=$((SUCCESS_TESTS + 1))
        else
            print_error "Deployment $(basename "$deployment") is invalid."
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    done

    # Test ConfigMaps generated from Python scripts.
    python_files=("${PYTHON_SCRIPTS_DIR}"/*.py)
    if [ ${#python_files[@]} -eq 0 ]; then
        echo "No Python files found in ${PYTHON_SCRIPTS_DIR}; skipping ConfigMap checks."
    else
        for file in "${python_files[@]}"; do
            filename="$(basename "$file")"
            base_name="${filename%.py}"
            configmap_name="$(clean_k8s_name "$base_name")"

            TOTAL_TESTS=$((TOTAL_TESTS + 1))
            if kubectl create configmap "$configmap_name" --from-file="$filename=$file" --dry-run=client -o yaml | kubectl -n "$NAMESPACE" apply --dry-run=client -f - >/dev/null 2>&1; then
                print_success "ConfigMap ${configmap_name} (from ${filename}) is valid."
                SUCCESS_TESTS=$((SUCCESS_TESTS + 1))
            else
                print_error "ConfigMap ${configmap_name} (from ${filename}) is invalid."
                FAILED_TESTS=$((FAILED_TESTS + 1))
            fi
        done
    fi

    # Test that the expected pods are ready (not just phase=Running).
    expected_pods=$(kubectl -n "$NAMESPACE" get deployments -o jsonpath='{.items[*].metadata.name}')
    for pod in $expected_pods; do
        TOTAL_TESTS=$((TOTAL_TESTS + 1))
        if kubectl -n "$NAMESPACE" wait --for=condition=Ready pod -l "app=$pod" --timeout=1s >/dev/null 2>&1; then
            print_success "Pod(s) for $pod are ready."
            SUCCESS_TESTS=$((SUCCESS_TESTS + 1))
        else
            status_line="$(kubectl -n "$NAMESPACE" get pods -l "app=$pod" -o custom-columns='WAITING_REASON:.status.containerStatuses[*].state.waiting.reason' --no-headers 2>/dev/null || echo "Not Found")"
            print_error "Pod(s) for $pod are not ready. Current status: ${status_line:-Not Found}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    done
fi
if [ "$ARG1" == "-s" ] || [ "$ARG1" == "--scripts" ] || [ "$ARG1" == "-a" ] || [ "$ARG1" == "--all" ]; then
    echo "Running script unit tests"

    python_files=("${PYTHON_SCRIPTS_DIR}"/*.py)
    if [ ${#python_files[@]} -eq 0 ]; then
        TOTAL_TESTS=$((TOTAL_TESTS + 1))
        print_error "Then FAIL: no Python scripts found in ${PYTHON_SCRIPTS_DIR}."
        FAILED_TESTS=$((FAILED_TESTS + 1))
    else
        for file in "${python_files[@]}"; do
            run_script_log_unit_test "$file"
        done
    fi
fi

print_result "Tests passed: $SUCCESS_TESTS/$TOTAL_TESTS"