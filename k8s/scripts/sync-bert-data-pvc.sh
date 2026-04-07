#!/bin/bash

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

NAMESPACE="${EAER_DATA_NAMESPACE:-argo}"
PVC_NAME="${EAER_BERT_DATA_PVC:-pipeline-bert-data-claim}"
LOCAL_DATA_DIR="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/Data_example/bert"
PVC_MANIFEST="${ROOT_DIR}/k8s/argo/pvc-bert-data.yaml"
SYNC_POD="bert-data-sync-$$"

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

if [ ! -d "$LOCAL_DATA_DIR" ]; then
    echo "Local data directory not found: $LOCAL_DATA_DIR"
    exit 1
fi

kubectl apply -f "$PVC_MANIFEST"

kubectl -n "$NAMESPACE" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${SYNC_POD}
spec:
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
kubectl cp "$LOCAL_DATA_DIR/." "$NAMESPACE/$SYNC_POD:/data"

echo "Synced CSV files from ${LOCAL_DATA_DIR} to PVC ${PVC_NAME} in namespace ${NAMESPACE}."