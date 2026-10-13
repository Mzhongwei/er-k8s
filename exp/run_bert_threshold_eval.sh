#!/usr/bin/env bash
# Score a finished run's embedding decision with its BERT checkpoint and report how the BERT
# threshold trades true pairs against false ones (exp/bert_threshold_eval.py).
#
# Usage, on the NFS server (server2), from the repository root:
#   bash exp/run_bert_threshold_eval.sh <run_dir> [dataset] [limit]
#     run_dir  k8s/results/embedding-training-inference-evaluation-...  (has matching/predicted/)
#     dataset  retrieval-extrait (default) or 8-movie; selects /data/rtas/<dataset>/ and the seed
#     limit    optional: score a random sample of this many pairs first (quick check)
#   RUN_VERSION=bench-<dataset of the run>  when the run used another dataset than the model,
#     e.g. a retrieval-extrait run scored with the retrieval-extrait-hn model: pairs whose
#     records are not in <dataset>'s tables (held out for training) are then skipped.
#
# Runs one Job with the pipeline's BERT image and one GPU on $NODE (default server2-labo). It
# waits for the GPU, so do not run it while a pipeline holds the GPU. The model comes from the
# seed store (<seed>/bert/bench-<dataset>), the tables from the dataset volume. Results:
#   <run_dir>/bert-threshold-eval/{report.json,pairs.csv,log.txt}
set -euo pipefail

RUN_DIR="$(cd "${1:?usage: $0 <run_dir> [dataset] [limit]}" && pwd)"
DATASET="${2:-retrieval-extrait}"
LIMIT="${3:-}"
NAMESPACE="${NAMESPACE:-argo}"
NODE="${NODE:-server2-labo}"
SEED="${SEED:-serie1-${DATASET}-s2}"
VERSION="bench-${DATASET}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JOB="bert-threshold-eval"

RUN_VERSION="${RUN_VERSION:-$VERSION}"
PAIRS="$RUN_DIR/matching/predicted/$RUN_VERSION/predicted_matching.csv"
[ -f "$PAIRS" ] || { echo "decision pairs not found: $PAIRS" >&2; exit 1; }
SEED_HOST_DIR="$(awk '/^[[:space:]]*path:/ {print $2; exit}' "$ROOT/k8s/datasets/seed-volume.yaml")"
MODEL_HOST_DIR="$SEED_HOST_DIR/$SEED/bert/$VERSION"
[ -f "$MODEL_HOST_DIR/config.json" ] || {
    echo "BERT checkpoint not found: $MODEL_HOST_DIR (set SEED=...; ls $SEED_HOST_DIR/$SEED/bert)" >&2
    exit 1
}
IMAGE="$(tr -d '[:space:]' < "$ROOT/k8s/images/image-repository.conf"):bert"

# The decision pairs reach the Pod through the (writable) seed volume.
WORK_NAME="_eval/${JOB}-$(date +%Y%m%d-%H%M%S)"
WORK_HOST_DIR="$SEED_HOST_DIR/$WORK_NAME"
SUDO=""; [ -w "$SEED_HOST_DIR" ] || SUDO="sudo"
$SUDO mkdir -p "$WORK_HOST_DIR"
$SUDO cp "$PAIRS" "$WORK_HOST_DIR/predicted_matching.csv"
$SUDO chmod -R a+rwX "$WORK_HOST_DIR"

kubectl delete job -n "$NAMESPACE" "$JOB" --ignore-not-found=true --wait=true >/dev/null
kubectl create configmap -n "$NAMESPACE" "$JOB-script" \
    --from-file=bert_threshold_eval.py="$ROOT/exp/bert_threshold_eval.py" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

LIMIT_ARGS=""
[ -n "$LIMIT" ] && LIMIT_ARGS="\"--limit\", \"$LIMIT\","
[ "$RUN_VERSION" != "$VERSION" ] && LIMIT_ARGS="$LIMIT_ARGS \"--skip-missing\","
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
      # As compiler.py does for GPU Pods: k3s keeps runc as the default runtime, which does
      # not expose the GPU even when one is allocated.
      runtimeClassName: nvidia
      nodeSelector: {kubernetes.io/hostname: "$NODE"}
      tolerations:
        - {key: nvidia.com/gpu, operator: Exists, effect: NoSchedule}
      containers:
        - name: main
          image: $IMAGE
          imagePullPolicy: IfNotPresent
          command: ["/opt/venv/bin/python", "/work/bert_threshold_eval.py",
                    "--model-dir", "/seeds/$SEED/bert/$VERSION",
                    "--pairs", "/seeds/$WORK_NAME/predicted_matching.csv",
                    "--matches", "/data/rtas/$DATASET/matches.txt",
                    "--table-a", "/data/rtas/$DATASET/table_A.csv",
                    "--table-b", "/data/rtas/$DATASET/table_B.csv",
                    $LIMIT_ARGS
                    "--device", "cuda",
                    "--output", "/seeds/$WORK_NAME/out"]
          resources:
            requests: {cpu: "2", memory: 4Gi}
            limits: {cpu: "4", memory: 12Gi, nvidia.com/gpu: 1}
          volumeMounts:
            - {name: script, mountPath: /work}
            - {name: seeds, mountPath: /seeds}
            - {name: data, mountPath: /data, readOnly: true}
      volumes:
        - {name: script, configMap: {name: $JOB-script}}
        - {name: seeds, persistentVolumeClaim: {claimName: pipeline-seed-claim}}
        - {name: data, persistentVolumeClaim: {claimName: pipeline-data-claim, readOnly: true}}
EOF

echo "Waiting for $JOB (needs a free GPU on $NODE)..."
until kubectl get pods -n "$NAMESPACE" -l job-name="$JOB" -o jsonpath='{.items[0].status.phase}' 2>/dev/null \
        | grep -qE 'Running|Succeeded|Failed'; do
    sleep 5
done
kubectl logs -n "$NAMESPACE" -f "job/$JOB" || true
kubectl wait -n "$NAMESPACE" --for=condition=complete "job/$JOB" --timeout=60s >/dev/null 2>&1 \
    || { echo "$JOB did not succeed; see: kubectl describe job -n $NAMESPACE $JOB" >&2; exit 1; }

OUT="$RUN_DIR/bert-threshold-eval"
mkdir -p "$OUT"
cp "$WORK_HOST_DIR/out/report.json" "$WORK_HOST_DIR/out/pairs.csv" "$OUT/"
kubectl logs -n "$NAMESPACE" "job/$JOB" > "$OUT/log.txt" 2>&1 || true
$SUDO rm -rf "$WORK_HOST_DIR"
kubectl delete configmap -n "$NAMESPACE" "$JOB-script" --ignore-not-found=true >/dev/null
echo "Results: $OUT/report.json and $OUT/pairs.csv"
