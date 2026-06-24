#!/bin/bash

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

NAMESPACE="${EAER_DATA_NAMESPACE:-argo}"
PVC_NAME="${EAER_BERT_DATA_PVC:-pipeline-data-claim}"
LOCAL_BERT_DATA_DIR="/srv/shared/data/exp_datasets/4-1_dirty_dblp_acm"
GROUND_TRUTH_FILE="/srv/shared/data/exp_datasets/4-1_dirty_dblp_acm/matches.txt"
FODORS_TABLE_A_FILE="/srv/shared/data/exp_datasets/4-1_dirty_dblp_acm/tableA.csv"
FODORS_TABLE_B_FILE="/srv/shared/data/exp_datasets/4-1_dirty_dblp_acm/tableB.csv"
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

if [ ! -d "$LOCAL_BERT_DATA_DIR" ]; then
  echo "Local BERT data directory not found: $LOCAL_BERT_DATA_DIR"
    exit 1
fi

if [ ! -f "$GROUND_TRUTH_FILE" ]; then
  echo "Ground-truth file not found: $GROUND_TRUTH_FILE"
  exit 1
fi

if [ ! -f "$FODORS_TABLE_A_FILE" ]; then
  echo "Fodors table A file not found: $FODORS_TABLE_A_FILE"
  exit 1
fi

if [ ! -f "$FODORS_TABLE_B_FILE" ]; then
  echo "Fodors table B file not found: $FODORS_TABLE_B_FILE"
  exit 1
fi

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
                  - server1-labo
  restartPolicy: Never
  containers:
  - name: ${SYNC_POD}
    image: busybox:1.36
    command: ["sh", "-c", "sleep 600"]
    volumeMounts:
    - name: bert-data
      mountPath: /data
  volumes:
  - name: bert-data
    persistentVolumeClaim:
      claimName: ${PVC_NAME}
EOF

kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/${SYNC_POD}" --timeout=120s >/dev/null
kubectl -n "$NAMESPACE" exec "$SYNC_POD" -- sh -c 'rm -rf /data/*'
kubectl -n "$NAMESPACE" exec "$SYNC_POD" -- sh -c 'mkdir -p /data/exp_datasets/4-1_dirty_dblp_acm/'
kubectl cp "$LOCAL_BERT_DATA_DIR/." "$NAMESPACE/$SYNC_POD:/data/exp_datasets/4-1_dirty_dblp_acm"
kubectl cp "$GROUND_TRUTH_FILE" "$NAMESPACE/$SYNC_POD:/data/exp_datasets/4-1_dirty_dblp_acm/matches.txt"
kubectl cp "$FODORS_TABLE_A_FILE" "$NAMESPACE/$SYNC_POD:/data/exp_datasets/4-1_dirty_dblp_acm/tableA.csv"
kubectl cp "$FODORS_TABLE_B_FILE" "$NAMESPACE/$SYNC_POD:/data/exp_datasets/4-1_dirty_dblp_acm/tableB.csv"

echo "Synced BERT and Fodors CSV files to PVC ${PVC_NAME} in namespace ${NAMESPACE}."