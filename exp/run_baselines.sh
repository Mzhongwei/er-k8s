#!/usr/bin/env bash
# Baseline experiments of the current pipeline (decision off, BERT threshold and one-match
# selection per dataset as in exp/serie1-motivation.py DATASETS), all on server2, with RAPL/NVML
# energy (idle baseline before and after each phase) and Alumet as a cross-check:
#
#   single    8-movie, then retrieval-extrait-hn: the fastest configuration, once
#             (8-movie first: a 20-minute check of images, energy and selection before the
#             ~13-hour retrieval run)                         -> reports/baseline-single-s2
#   sweep     8-movie: one stage at a time (random-walk processes, Gensim workers, BERT
#             device/cores) around the baseline point: stage leverage (Observation 1)
#                                                             -> reports/baseline-sweep-s2
#   capacity  8-movie: complete configurations (BERT cpu/gpu x Word2Vec workers): the least-
#             energy feasible one per deadline (Observation 3) -> reports/baseline-capacity-s2
#   server1   8-movie and retrieval-extrait-hn on server1 (placement, Observation 2; needs the
#             VM's DNS for image pulls)                       -> reports/baseline-single-s1
#
# Usage, on server2 from the repository root, after `helm upgrade` of Alumet:
#   nohup bash exp/run_baselines.sh > baselines.log 2>&1 &
# Environment: PHASES (default "single sweep capacity"), REPETITIONS (sweep/capacity, default 3),
# RECORD_INTERVAL_MS (20), DECISION (off), SKIP_IMAGE_BUILD (0: the first phase builds and
# pushes the images, later phases reuse them). Each phase resumes: rerunning skips trials
# already recorded as Succeeded in its runs.csv.
set -uo pipefail
cd "$(dirname "$0")/.."

PHASES="${PHASES:-single sweep capacity}"
REPETITIONS="${REPETITIONS:-3}"
RECORD_INTERVAL_MS="${RECORD_INTERVAL_MS:-20}"
DECISION="${DECISION:-off}"
build="${SKIP_IMAGE_BUILD:-0}"
log() { echo "[$(date '+%F %T')] $*"; }

phase() {
    local name="$1" node="$2" monitor="$3"; shift 3
    log "phase $name: start"
    ERCTL_ALUMET_NODES="$node" ENERGY_MONITOR="$monitor" SKIP_IMAGE_BUILD="$build" \
        bash ./bootstrap-serie1.sh run --nodes "${node%%-*}" --decision-making "$DECISION" \
        --record-interval-ms "$RECORD_INTERVAL_MS" --rapl --keep-going "$@"
    local code=$?
    build=1  # images are built (or reused) by the first phase only
    log "phase $name: exit code $code"
    return $code
}

for name in $PHASES; do
    case "$name" in
        single)
            phase single server2-labo alumet --design single \
                --datasets 8-movie,retrieval-extrait-hn --output reports/baseline-single-s2 ;;
        sweep)
            phase sweep server2-labo alumet --design sweep --datasets 8-movie \
                --repetitions "$REPETITIONS" --shuffle-seed 1 --output reports/baseline-sweep-s2 ;;
        capacity)
            phase capacity server2-labo alumet --design capacity --datasets 8-movie \
                --repetitions "$REPETITIONS" --shuffle-seed 2 --output reports/baseline-capacity-s2 ;;
        server1)
            # Alumet and EcoFLOC do not work inside the VM: RAPL on its host and NVML only.
            phase server1 server1-k3s-worker none --design single \
                --datasets 8-movie,retrieval-extrait-hn --output reports/baseline-single-s1 ;;
        *) log "unknown phase $name (single, sweep, capacity, server1)"; exit 2 ;;
    esac
done
log "done: reports/baseline-*"
