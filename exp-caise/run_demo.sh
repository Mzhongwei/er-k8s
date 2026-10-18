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
        # The BERT image's pins (transformers 4.30.2, tokenizers 0.13.3) have wheels up to
        # Python 3.11 only; newer Pythons build tokenizers from source (needs Rust).
        if [ -x "$VENV/bin/python" ] && ! "$VENV/bin/python" -c 'import sys; sys.exit(sys.version_info[:2] != (3, 11))'; then
            echo "$VENV is not Python 3.11: remove it (rm -rf $VENV) and rerun setup" >&2
            exit 1
        fi
        if [ ! -x "$VENV/bin/python" ]; then
            if command -v "${PYTHON:-python3.11}" >/dev/null; then
                "${PYTHON:-python3.11}" -m venv "$VENV"
            elif command -v uv >/dev/null; then
                uv venv --seed --python 3.11 "$VENV"  # uv fetches a standalone Python 3.11
            else
                echo "Python 3.11 not found: install python3.11 (with venv), set PYTHON=/path/to/python3.11," >&2
                echo "or install uv (https://docs.astral.sh/uv/) so setup can fetch one" >&2
                exit 1
            fi
        fi
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
