#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PIPELINE_FILE="${ROOT_DIR}/k8s/argo/pipeline.yaml"
CONFIGMAPS_FILE="${ROOT_DIR}/k8s/scripts/configmaps.sh"

fail() {
  echo "[FAIL] $1" >&2
  exit 1
}

check_contains() {
  local file="$1"
  local pattern="$2"
  local message="$3"
  if ! grep -Fq -- "$pattern" "$file"; then
    fail "$message"
  fi
}

check_absent() {
  local file="$1"
  local pattern="$2"
  local message="$3"
  if grep -Fq -- "$pattern" "$file"; then
    fail "$message"
  fi
}

check_contains "$CONFIGMAPS_FILE" '"eaer-graph-distribution" "graph_randomwalk.py"' \
  "ConfigMap graph key must be graph_randomwalk.py"

check_contains "$PIPELINE_FILE" 'command: [/opt/venv/bin/python, /app/distributions/graph_randomwalk.py]' \
  "Graph template must execute graph_randomwalk.py"
check_contains "$PIPELINE_FILE" 'subPath: graph_randomwalk.py' \
  "Graph template mount subPath must be graph_randomwalk.py"

check_absent "$PIPELINE_FILE" 'graph_distribution.py' \
  "Legacy graph_distribution.py reference still present in workflow"

check_contains "$PIPELINE_FILE" '    - name: graph' \
  "Graph template missing"
check_contains "$PIPELINE_FILE" '    - name: feature-index' \
  "Feature-index template missing"
check_contains "$PIPELINE_FILE" '    - name: embedding' \
  "Embedding template missing"
check_contains "$PIPELINE_FILE" '    - name: prediction' \
  "Prediction template missing"

check_contains "$PIPELINE_FILE" "- --mode" \
  "Expected --mode argument wiring in workflow templates"

check_contains "$PIPELINE_FILE" "value: '{{tasks.pipeline-init.outputs.parameters.mode}}'" \
  "Expected DAG mode propagation from pipeline-init output"

echo "[OK] Workflow contract checks passed"
