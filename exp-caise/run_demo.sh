#!/usr/bin/env bash
# TF-IDF + BERT demo (exp-caise/run_demo.py) on a GPU node, outside Kubernetes.
#
#   bash exp-caise/run_demo.sh setup              # once: Python venv with the BERT image's packages
#   bash exp-caise/run_demo.sh check              # every dataset's files in place under DATA_ROOT?
#   nohup bash exp-caise/run_demo.sh run > caise-demo.log 2>&1 &
#   bash exp-caise/run_demo.sh run --datasets abt_buy,movie --threshold 0.7
#
# Environment: VENV (~/caise-venv), DATA_ROOT (/data: holds exp_datasets/ and rtas/, see
# datasets.yaml), OUTPUT (reports/caise-demo-<time>). Arguments after `run` go to run_demo.py.
# The GPU must be free: stop or drain Pods that hold it (`nvidia-smi`) before a run.
set -euo pipefail
cd "$(dirname "$0")/.."

VENV="${VENV:-$HOME/caise-venv}"
DATA_ROOT="${DATA_ROOT:-/data}"
OUTPUT="${OUTPUT:-reports/caise-demo-$(date +%Y%m%d-%H%M%S)}"
REQUIREMENTS=code/Energy-Aware-Entity-Resolution/requirements/for_entries

case "${1:-}" in
    setup)
        python3 -m venv "$VENV"
        # Same versions as docker/Dockerfile.bert; scipy as in the embedding image (TF-IDF).
        "$VENV/bin/pip" install --upgrade pip
        "$VENV/bin/pip" install torch==2.5.1+cu121 --extra-index-url https://download.pytorch.org/whl/cu121
        "$VENV/bin/pip" install -r "$REQUIREMENTS/requirements-bert.txt" scipy==1.13.1
        # Downloads bert-base-uncased into the Hugging Face cache once.
        "$VENV/bin/python" -c 'from transformers import AutoModelForSequenceClassification, AutoTokenizer; \
AutoModelForSequenceClassification.from_pretrained("bert-base-uncased", num_labels=2); \
AutoTokenizer.from_pretrained("bert-base-uncased")'
        "$VENV/bin/python" -c 'import torch; print("CUDA available:", torch.cuda.is_available())'
        ;;
    check)
        shift
        "$VENV/bin/python" exp-caise/run_demo.py --data-root "$DATA_ROOT" --check "$@"
        ;;
    run)
        shift
        nvidia-smi --query-gpu=name,memory.used,memory.total --format=csv || true
        OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 "$VENV/bin/python" exp-caise/run_demo.py \
            --data-root "$DATA_ROOT" --output "$OUTPUT" --device cuda "$@"
        ;;
    *)
        echo "usage: $0 setup | check [--datasets ...] | run [run_demo.py arguments]" >&2
        exit 2
        ;;
esac
