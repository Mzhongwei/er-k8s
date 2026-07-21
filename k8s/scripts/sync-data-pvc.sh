#!/bin/bash

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Select and copy exactly one dataset family. No argument follows the global default and
# means embedding; switching mode clears the previous PVC contents before copying the
# selected files, so stale files from another mode cannot be consumed accidentally.
if [ $# -gt 1 ]; then
    echo "Usage: erctl dataset [embedding|bert]"
    exit 1
fi

DATA_MODE="${1:-embedding}"
if [ "$DATA_MODE" != "embedding" ] && [ "$DATA_MODE" != "bert" ]; then
    echo "Invalid mode: $DATA_MODE. Use 'embedding' or 'bert'."
    exit 1
fi

NAMESPACE="${EAER_DATA_NAMESPACE:-argo}"
PVC_NAME="${EAER_DATA_PVC:-${EAER_BERT_DATA_PVC:-pipeline-data-claim}}"
LOCAL_DATA_DIR="${EAER_LOCAL_DATA_DIR:-/srv/shared/data/exp_datasets/4-1_dirty_dblp_acm}"

# embedding mode: entity-matching pairs, read via config-embedding.yaml's
# data_source_A/data_source_B/ground_truth.
GROUND_TRUTH_FILE="${LOCAL_DATA_DIR}/matches.txt"
FODORS_TABLE_A_FILE="${LOCAL_DATA_DIR}/tableA.csv"
FODORS_TABLE_B_FILE="${LOCAL_DATA_DIR}/tableB.csv"

# bert mode: labeled train/test/valid splits, read via config-bert.yaml's
# trainset_path/testset_path/evalset_path.
BERT_TRAIN_FILE="${LOCAL_DATA_DIR}/train.csv"
BERT_TEST_FILE="${LOCAL_DATA_DIR}/test.csv"
BERT_VALID_FILE="${LOCAL_DATA_DIR}/valid.csv"

PVC_MANIFEST="${ROOT_DIR}/k8s/pvc-manifests/pvc-data.yaml"
SYNC_POD="data-sync-$$"

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found: $cmd"
    exit 1
  fi
}

require_cmd kubectl

cleanup() {
  kubectl -n "$NAMESPACE" delete pod "$SYNC_POD" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
}

trap cleanup EXIT

require_file() {
  local label="$1"
  local path="$2"
  if [ ! -f "$path" ]; then
    echo "$label file not found: $path"
    exit 1
  fi
}

if [ ! -d "$LOCAL_DATA_DIR" ]; then
  echo "Local dataset directory not found: $LOCAL_DATA_DIR"
  exit 1
fi

case "$DATA_MODE" in
  embedding)
    require_file "Ground-truth" "$GROUND_TRUTH_FILE"
    require_file "Embedding table A" "$FODORS_TABLE_A_FILE"
    require_file "Embedding table B" "$FODORS_TABLE_B_FILE"
    DATASET_FILES=(
      "$GROUND_TRUTH_FILE"
      "$FODORS_TABLE_A_FILE"
      "$FODORS_TABLE_B_FILE"
    )
    ;;
  bert)
    require_file "BERT train set" "$BERT_TRAIN_FILE"
    require_file "BERT test set" "$BERT_TEST_FILE"
    require_file "BERT valid set" "$BERT_VALID_FILE"
    DATASET_FILES=(
      "$BERT_TRAIN_FILE"
      "$BERT_TEST_FILE"
      "$BERT_VALID_FILE"
    )
    ;;
esac

kubectl apply -f "$PVC_MANIFEST"

kubectl -n "$NAMESPACE" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${SYNC_POD}
spec:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: kubernetes.io/hostname
                operator: NotIn
                values:
                  - matis-asus-expertbook-b1500ceaey-b1500ceae
                  - zhongwei-lap
  restartPolicy: Never
  containers:
  - name: ${SYNC_POD}
    image: busybox:1.36
    command: ["sh", "-c", "sleep 600"]
    volumeMounts:
    - name: pipeline-data
      mountPath: /data
  volumes:
  - name: pipeline-data
    persistentVolumeClaim:
      claimName: ${PVC_NAME}
EOF

kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/${SYNC_POD}" --timeout=120s >/dev/null
kubectl -n "$NAMESPACE" exec "$SYNC_POD" -- sh -c 'rm -rf /data/*'
kubectl -n "$NAMESPACE" exec "$SYNC_POD" -- sh -c 'mkdir -p /data/exp_datasets/4-1_dirty_dblp_acm/'

for dataset_file in "${DATASET_FILES[@]}"; do
  kubectl cp \
    "$dataset_file" \
    "$NAMESPACE/$SYNC_POD:/data/exp_datasets/4-1_dirty_dblp_acm/$(basename "$dataset_file")"
done

echo "Synced ${DATA_MODE} dataset files to PVC ${PVC_NAME} in namespace ${NAMESPACE}."
