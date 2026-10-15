#!/usr/bin/env bash
# Candidate recall of the embedding blocking (exp/blocking_recall.py): for every true pair,
# the rank of A in B's similarity list and of B in A's window, with the last run's final
# embedding model (the 'current' checkpoint on the embedding cache volume).
#
# Usage, on server2 from the repository root, while no pipeline runs (the next run replaces
# the model):
#   bash exp/run_blocking_recall.sh [dataset] [window_count]
#     dataset       retrieval-extrait-hn (default); selects /data/rtas/<dataset>/ and the
#                   model directory /app/data/embedding/bench-<dataset>
#     window_count  records per window of that run (default 6750)
# CPU only. Results: reports/blocking-recall-<dataset>-<time>/{report.json,ranks.csv,log.txt}
# Writes under <seed volume>/_eval (see exp/run_bert_threshold_eval.sh to make it writable).
set -euo pipefail

DATASET="${1:-retrieval-extrait-hn}"
WINDOW_COUNT="${2:-6750}"
NAMESPACE="${NAMESPACE:-argo}"
NODE="${NODE:-server2-labo}"
VERSION="${VERSION:-bench-${DATASET}}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JOB="blocking-recall"
IMAGE="$(tr -d '[:space:]' < "$ROOT/k8s/images/image-repository.conf"):embedding"
SEED_HOST_DIR="$(awk '/^[[:space:]]*path:/ {print $2; exit}' "$ROOT/k8s/datasets/seed-volume.yaml")"
WORK_NAME="_eval/${JOB}-$(date +%Y%m%d-%H%M%S)"
WORK_HOST_DIR="$SEED_HOST_DIR/$WORK_NAME"
SUDO=""; [ -w "$SEED_HOST_DIR/_eval" ] || SUDO="sudo"
$SUDO mkdir -p "$WORK_HOST_DIR" && $SUDO chmod a+rwx "$WORK_HOST_DIR"

kubectl delete job -n "$NAMESPACE" "$JOB" --ignore-not-found=true --wait=true >/dev/null
kubectl create configmap -n "$NAMESPACE" "$JOB-script" \
    --from-file=blocking_recall.py="$ROOT/exp/blocking_recall.py" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -n "$NAMESPACE" -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      nodeSelector: {kubernetes.io/hostname: "$NODE"}
      containers:
        - name: main
          image: $IMAGE
          imagePullPolicy: IfNotPresent
          command: ["/opt/venv/bin/python", "/work/blocking_recall.py",
                    "--model", "/app/data/embedding/$VERSION",
                    "--table-a", "/data/rtas/$DATASET/table_A.csv",
                    "--table-b", "/data/rtas/$DATASET/table_B.csv",
                    "--matches", "/data/rtas/$DATASET/matches.txt",
                    "--window-count", "$WINDOW_COUNT",
                    "--output", "/seeds/$WORK_NAME"]
          env:
            - {name: OMP_NUM_THREADS, value: "8"}
            - {name: OPENBLAS_NUM_THREADS, value: "8"}
          resources:
            requests: {cpu: "4", memory: 8Gi}
            limits: {cpu: "8", memory: 24Gi}
          volumeMounts:
            - {name: script, mountPath: /work}
            - {name: embedding, mountPath: /app/data/embedding, readOnly: true}
            - {name: seeds, mountPath: /seeds}
            - {name: data, mountPath: /data, readOnly: true}
      volumes:
        - {name: script, configMap: {name: $JOB-script}}
        - {name: embedding, persistentVolumeClaim: {claimName: pipeline-embedding-model-cache-claim, readOnly: true}}
        - {name: seeds, persistentVolumeClaim: {claimName: pipeline-seed-claim}}
        - {name: data, persistentVolumeClaim: {claimName: pipeline-data-claim, readOnly: true}}
EOF

echo "Waiting for $JOB..."
until kubectl get pods -n "$NAMESPACE" -l job-name="$JOB" -o jsonpath='{.items[0].status.phase}' 2>/dev/null \
        | grep -qE 'Running|Succeeded|Failed'; do
    sleep 5
done
kubectl logs -n "$NAMESPACE" -f "job/$JOB" || true
kubectl wait -n "$NAMESPACE" --for=condition=complete "job/$JOB" --timeout=60s >/dev/null 2>&1 \
    || { echo "$JOB did not succeed; see: kubectl describe job -n $NAMESPACE $JOB" >&2; exit 1; }

OUT="$ROOT/reports/blocking-recall-$DATASET-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
cp "$WORK_HOST_DIR/report.json" "$WORK_HOST_DIR/ranks.csv" "$OUT/"
kubectl logs -n "$NAMESPACE" "job/$JOB" > "$OUT/log.txt" 2>&1 || true
$SUDO rm -rf "$WORK_HOST_DIR" 2>/dev/null || echo "note: could not remove $WORK_HOST_DIR" >&2
kubectl delete configmap -n "$NAMESPACE" "$JOB-script" --ignore-not-found=true >/dev/null
echo "Results: $OUT/report.json and $OUT/ranks.csv"
