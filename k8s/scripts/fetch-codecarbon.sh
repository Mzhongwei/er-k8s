#!/usr/bin/env bash

# Fetch CodeCarbon emissions CSV for a workflow by mounting the shared PVC
# and copying /app/data/codecarbon/<workflow>/emissions.csv to local dir.
# Usage: fetch-codecarbon.sh --workflow <workflow-name> [--dest <local-dir>]

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi
set -euo pipefail

WORKFLOW_NAME=""
DEST_DIR="./reports"
NAMESPACE="argo"
PVC_NAME="pipeline-reports-claim"
POD_NAME="erctl-copy-$(date +%s)"
TIMEOUT=${TIMEOUT:-60}

print_help() {
  cat <<'EOF'
Usage: fetch-codecarbon.sh [--workflow WORKFLOW_NAME] [--dest LOCAL_DIR] [--namespace NAMESPACE]

Options:
  --workflow WORKFLOW_NAME   Workflow name to fetch emissions for. If omitted, the most recent workflow in namespace is used.
  --dest LOCAL_DIR           Local destination directory (default: ./reports)
  --namespace NAMESPACE      Kubernetes namespace (default: argo)
  --help                    Show this help

Example:
  fetch-codecarbon.sh --workflow pipeline-abcde --dest ~/Downloads
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --workflow)
      WORKFLOW_NAME="$2"
      shift 2
      ;;
    --dest)
      DEST_DIR="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      print_help
      exit 1
      ;;
  esac
done

if [ -z "$WORKFLOW_NAME" ]; then
  echo "No workflow provided; selecting most recent workflow in namespace $NAMESPACE"
  WORKFLOW_NAME=$(kubectl get wf -n "$NAMESPACE" --no-headers -o custom-columns=:metadata.name | tail -n 1 || true)
  if [ -z "$WORKFLOW_NAME" ]; then
    echo "No workflows found in namespace $NAMESPACE; please provide --workflow"
    exit 1
  fi
fi

REMOTE_PATH="/app/reports/codecarbon/${WORKFLOW_NAME}/emissions.csv"
LOCAL_PATH="${DEST_DIR%/}/emissions-${WORKFLOW_NAME}.csv"

cleanup() {
  kubectl delete pod "$POD_NAME" -n "$NAMESPACE" --ignore-not-found=true >/dev/null 2>&1 || true
}
trap cleanup EXIT

cat <<EOF | kubectl apply -n "$NAMESPACE" -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_NAME}
spec:
  containers:
  - name: tmp
    image: alpine:3.18
    command: ["/bin/sh","-c","sleep 3600"]
    volumeMounts:
    - name: pipeline-reports
      mountPath: /app/reports
  restartPolicy: Never
  volumes:
  - name: pipeline-reports
    persistentVolumeClaim:
      claimName: ${PVC_NAME}
EOF

echo "Pod manifest applied. Waiting 2s for pod registration..."
sleep 2

echo "Waiting for pod $POD_NAME to be running..."
if ! kubectl wait --for=condition=Ready pod/$POD_NAME -n "$NAMESPACE" --timeout=${TIMEOUT}s 2>&1 | grep -q "condition met"; then
  echo "Pod did not become ready within ${TIMEOUT}s; checking pod status..."
  kubectl get pod "$POD_NAME" -n "$NAMESPACE" -o wide || true
  exit 1
fi

echo "Pod is ready. Copying $REMOTE_PATH to $LOCAL_PATH..."

# Ensure destination directory exists
mkdir -p "$DEST_DIR"

# Retry logic for kubectl cp (sometimes needs a moment after pod is ready)
MAX_RETRIES=3
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  if kubectl cp "$NAMESPACE/$POD_NAME:$REMOTE_PATH" "$LOCAL_PATH" 2>/dev/null; then
    echo "Successfully copied to $LOCAL_PATH"
    if [ -f "$LOCAL_PATH" ]; then
      ls -lh "$LOCAL_PATH"
      exit 0
    fi
  fi
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
    echo "Copy attempt $RETRY_COUNT failed. Retrying in 2s..."
    sleep 2
  fi
done

# If we get here, all retries failed
echo "Failed to copy after $MAX_RETRIES attempts. The file may not exist: $REMOTE_PATH"
echo "Checking pod and files..."
kubectl exec -n "$NAMESPACE" "$POD_NAME" -- ls -la "/app/reports/codecarbon/${WORKFLOW_NAME}/" || echo "Directory listing failed"
exit 1
