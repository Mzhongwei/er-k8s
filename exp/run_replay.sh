#!/usr/bin/env bash
# E1b + E2: replay states saved by a run with --replay-windows, one Kubernetes Job per
# measurement on an otherwise idle node, with RAPL/NVML energy (k8s/monitoring/rapl).
#
#   Word2Vec: every saved state x every worker count, REPS repetitions per Job:
#             restore the model (E2), train the window, write the checkpoint (E1b)
#   BERT:     the candidates of one saved window on each device (no state: one window)
#
# Usage, on server2 from the repository root, after the run and before any other run (the
# next run recreates the volumes that hold the saved states):
#   nohup bash exp/run_replay.sh retrieval-extrait-hn > replay.log 2>&1 &
# Environment: STATES ("10 75 145"), WORKERS ("2 4 8 12"), REPS (2), BERT_STATE (75),
# BERT_DEVICES ("cuda cpu:8 cpu:12"), IDLE_SECONDS (300), NODE (server2-labo).
# Results: reports/replay-<dataset>-<time>/{replay.jsonl,pods.jsonl,rapl/,results.csv}
set -euo pipefail
cd "$(dirname "$0")/.."

DATASET="${1:-retrieval-extrait-hn}"
VERSION="bench-${DATASET}"
STATES="${STATES:-10 75 145}"
WORKERS="${WORKERS:-2 4 8 12}"
REPS="${REPS:-2}"
BERT_STATE="${BERT_STATE:-75}"
BERT_DEVICES="${BERT_DEVICES:-cuda cpu:8 cpu:12}"
IDLE_SECONDS="${IDLE_SECONDS:-300}"
NODE="${NODE:-server2-labo}"
NAMESPACE="${NAMESPACE:-argo}"
REPO="$(tr -d '[:space:]' < k8s/images/image-repository.conf)"
OUT="reports/replay-${DATASET}-$(date +%Y%m%d-%H%M%S)"
RAPL="python3 k8s/monitoring/rapl/rapl.py"
mkdir -p "$OUT"
log() { echo "[$(date '+%F %T')] $*"; }

kubectl create configmap -n "$NAMESPACE" replay-script --from-file=replay_stage.py=exp/replay_stage.py \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null

$RAPL start --out "$OUT/rapl" --nodes "$NODE"
trap '$RAPL stop --out "$OUT/rapl" >/dev/null 2>&1 || true' EXIT
idle() {
    log "idle $1: ${IDLE_SECONDS} s"
    $RAPL mark --out "$OUT/rapl" "$1-start"; sleep "$IDLE_SECONDS"; $RAPL mark --out "$OUT/rapl" "$1-end"
}

run_job() {  # name image cpus gpus args...
    local name="$1" image="$2" cpus="$3" gpus="$4"; shift 4
    local args="" arg
    for arg in "$@"; do args="$args \"$arg\","; done
    local runtime="" tolerations="" gpu_limit=""
    if [ "$gpus" != "0" ]; then
        runtime="runtimeClassName: nvidia"
        tolerations="tolerations: [{key: nvidia.com/gpu, operator: Exists, effect: NoSchedule}]"
        gpu_limit=", nvidia.com/gpu: $gpus"
    fi
    kubectl delete job -n "$NAMESPACE" "$name" --ignore-not-found=true --wait=true >/dev/null
    kubectl apply -n "$NAMESPACE" -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $name
  labels: {app: replay}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 86400
  template:
    spec:
      restartPolicy: Never
      nodeSelector: {kubernetes.io/hostname: "$NODE"}
      $runtime
      $tolerations
      containers:
        - name: main
          image: $image
          imagePullPolicy: IfNotPresent
          command: ["/opt/venv/bin/python", "/work/replay_stage.py", ${args%,}]
          env:
            - {name: OMP_NUM_THREADS, value: "$cpus"}
            - {name: OPENBLAS_NUM_THREADS, value: "$cpus"}
          resources:
            requests: {cpu: "$cpus", memory: 24Gi}
            limits: {cpu: "$cpus", memory: 24Gi$gpu_limit}
          volumeMounts:
            - {name: script, mountPath: /work}
            - {name: embedding, mountPath: /app/data/embedding}
            - {name: predicted, mountPath: /app/data/predicted}
            - {name: bert, mountPath: /app/data/bert}
      volumes:
        - {name: script, configMap: {name: replay-script}}
        - {name: embedding, persistentVolumeClaim: {claimName: pipeline-embedding-model-cache-claim}}
        - {name: predicted, persistentVolumeClaim: {claimName: pipeline-decision-evaluation-cache-claim}}
        - {name: bert, persistentVolumeClaim: {claimName: pipeline-bert-model-claim}}
EOF
    log "job $name started"
    if ! kubectl wait -n "$NAMESPACE" --for=condition=complete "job/$name" --timeout=3h >/dev/null 2>&1; then
        log "job $name did not complete:"; kubectl logs -n "$NAMESPACE" "job/$name" --tail=20 || true
        return 1
    fi
    kubectl logs -n "$NAMESPACE" "job/$name" | grep '^\[REPLAY\]' | sed 's/^\[REPLAY\] //' >> "$OUT/replay.jsonl"
    kubectl get pod -n "$NAMESPACE" -l job-name="$name" -o json | python3 -c '
import json, sys
pod = json.load(sys.stdin)["items"][0]
state = pod["status"]["containerStatuses"][0]["state"].get("terminated", {})
print(json.dumps({"job": sys.argv[1], "created": pod["metadata"]["creationTimestamp"],
                  "started": state.get("startedAt"), "finished": state.get("finishedAt")}))' "$name" >> "$OUT/pods.jsonl"
    log "job $name done"
}

idle idle-before
for state in $STATES; do
    for workers in $WORKERS; do
        run_job "replay-w2v-w${state}-n${workers}" "$REPO:embedding" "$workers" 0 \
            word2vec --state "/app/data/embedding/$VERSION/replay/w$state" --workers "$workers" \
            --reps "$REPS" --scratch "/app/data/embedding/$VERSION/replay/_scratch" --label "w$state"
    done
done
for device in $BERT_DEVICES; do
    cores="${device#cpu:}"; [ "$device" = cuda ] && cores=2
    gpus=0; [ "$device" = cuda ] && gpus=1
    run_job "replay-bert-$(echo "$device" | tr ':' '-')" "$REPO:bert" "$cores" "$gpus" \
        bert --pairs "/app/data/predicted/$VERSION/replay/candidates_w$BERT_STATE.csv" \
        --config "/app/data/embedding/$VERSION/replay/w$BERT_STATE/config.json" \
        --device "$device" --reps "$REPS" --label "w$BERT_STATE"
done
idle idle-after
$RAPL stop --out "$OUT/rapl"
trap - EXIT
kubectl delete configmap -n "$NAMESPACE" replay-script --ignore-not-found=true >/dev/null
python3 exp/replay_analyze.py "$OUT"
log "results in $OUT"
