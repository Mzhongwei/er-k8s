#!/usr/bin/env bash
# Unattended follow-up of the BERT retraining (offline preparation with --prep-only):
#   1. wait until no exp/serie1-motivation.py runs, then check that the preparation succeeded;
#   2. score the previous run's decision with the new BERT (exp/run_bert_threshold_eval.sh);
#   3. pick the threshold: among those keeping >= MIN_TRUE_RETENTION of the true pairs, the one
#      letting through the fewest false pairs; it must let through <= MAX_FALSE_PASS of them;
#   4. if it does (or RUN_ANYWAY=1, then with the best projected F1 threshold), run the benchmark
#      with that threshold, decision making off, images already built (SKIP_IMAGE_BUILD=1).
#
# Usage, on server2 from the repository root, before leaving:
#   sudo mkdir -p /srv/nfs/k8s/eaer-seeds/_eval && sudo chmod a+rwx /srv/nfs/k8s/eaer-seeds/_eval
#   nohup bash exp/overnight_hn.sh > overnight-hn.log 2>&1 &
# Environment: RUN (previous run directory), DATASET, MIN_TRUE_RETENTION (0.90),
# MAX_FALSE_PASS (0.10), RUN_ANYWAY (0), RECORD_INTERVAL_MS (20).
set -uo pipefail
cd "$(dirname "$0")/.."

RUN="${RUN:-k8s/results/embedding-training-inference-evaluation-20261013-134216-1018907}"
DATASET="${DATASET:-retrieval-extrait-hn}"
OUTPUT="reports/serie1-single-k3-bert-only"
MIN_TRUE_RETENTION="${MIN_TRUE_RETENTION:-0.90}"
MAX_FALSE_PASS="${MAX_FALSE_PASS:-0.10}"
RECORD_INTERVAL_MS="${RECORD_INTERVAL_MS:-20}"
log() { echo "[$(date '+%F %T')] $*"; }

log "1/4 waiting for the offline preparation to finish"
while pgrep -f "exp/serie1-motivation.py" >/dev/null; do sleep 60; done
PREP="$(python3 - "$DATASET" <<'EOF'
import csv, glob, sys
found = []
for path in glob.glob("reports/serie1-*/offline-prep.csv"):
    for row in csv.DictReader(open(path)):
        if row.get("dataset") == sys.argv[1] and row.get("status") == "Succeeded":
            found.append((row.get("finished_at", ""), path))
print(max(found)[1] if found else "")
EOF
)"
[ -n "$PREP" ] || { log "no successful offline preparation of $DATASET in reports/serie1-*/offline-prep.csv; stop"; exit 1; }
log "preparation recorded in $PREP"
mkdir -p "$OUTPUT"
[ -f "$OUTPUT/offline-prep.csv" ] || cp "$PREP" "$OUTPUT/offline-prep.csv"  # reuse the new seed

log "2/4 scoring $RUN with the $DATASET BERT"
RUN_VERSION=bench-retrieval-extrait bash exp/run_bert_threshold_eval.sh "$RUN" "$DATASET" \
    || { log "evaluation failed; stop"; exit 1; }
REPORT="$RUN/bert-threshold-eval-$DATASET/report.json"

log "3/4 choosing the threshold from $REPORT"
THRESHOLD="$(python3 - "$REPORT" "$MIN_TRUE_RETENTION" "$MAX_FALSE_PASS" "${RUN_ANYWAY:-0}" <<'EOF'
import json, sys
report = json.load(open(sys.argv[1]))
min_retention, max_pass, anyway = float(sys.argv[2]), float(sys.argv[3]), sys.argv[4] == "1"
rows = report["thresholds"]
print(f"AUC bert={report['bert_auc']:.4f} embedding={report['embedding_score_auc']:.4f}", file=sys.stderr)
for row in rows:
    print(f"  thr={row['threshold']:<6} true_ret={row['true_retention']:.3f} "
          f"false_pass={row['false_pass_rate']:.3f} proj_F1={row['projected_f1']:.3f}", file=sys.stderr)
eligible = [row for row in rows if row["true_retention"] >= min_retention]
best = min(eligible, key=lambda row: (row["false_pass_rate"], -row["threshold"])) if eligible else None
if best and best["false_pass_rate"] <= max_pass:
    print(f"PASS: threshold {best['threshold']} keeps {best['true_retention']:.3f} of true pairs, "
          f"lets {best['false_pass_rate']:.3f} of false pairs through", file=sys.stderr)
    print(best["threshold"])
elif anyway:
    fallback = max(rows, key=lambda row: row["projected_f1"])
    print(f"NOT MET, RUN_ANYWAY=1: best projected F1 threshold {fallback['threshold']}", file=sys.stderr)
    print(fallback["threshold"])
else:
    print(f"NOT MET: no threshold keeps >= {min_retention} of true pairs with <= {max_pass} false pass",
          file=sys.stderr)
EOF
)"
[ -n "$THRESHOLD" ] || { log "criteria not met; benchmark not started (see the table above)"; exit 0; }

log "4/4 benchmark: $DATASET, decision making off, BERT threshold $THRESHOLD"
ERCTL_ALUMET_NODES=server2-labo SKIP_IMAGE_BUILD=1 bash ./bootstrap-serie1.sh run --datasets "$DATASET" --decision-making off \
    --bert-threshold "$THRESHOLD" --record-interval-ms "$RECORD_INTERVAL_MS" --keep-going --output "$OUTPUT"
log "benchmark finished with exit code $?; results in $OUTPUT"
