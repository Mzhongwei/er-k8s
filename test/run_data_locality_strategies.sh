#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    cat <<'EOF'
Usage: test/run_data_locality_strategies.sh <pipeline-config-directory> [pipeline start options]

Run every YAML pipeline configuration in the directory once for each strategy DL1-DL8.
Configurations are run in filename order by the existing `erctl pipeline start -d` command.
Do not pass -c/--config, -d/--directory, or --data-locality in the extra options.
EOF
}

if [ $# -lt 1 ]; then
    usage >&2
    exit 1
fi

case "$1" in
    -h|--help|-help|help)
        usage
        exit 0
        ;;
esac

CONFIG_DIR="$1"
shift
EXTRA_ARGS=("$@")

[ -d "$CONFIG_DIR" ] || { echo "Config directory not found: $CONFIG_DIR" >&2; exit 1; }

for argument in "${EXTRA_ARGS[@]}"; do
    case "$argument" in
        -c|--config|--config=*|-d|--directory|--directory=*|--data-locality|--data-locality=*)
            echo "Do not pass config, directory, or data-locality options through extra arguments: $argument" >&2
            exit 1
            ;;
    esac
done

for strategy in DL1 DL2 DL3 DL4 DL5 DL6 DL7 DL8; do
    echo "=== Running data-locality strategy $strategy ==="
    bash "$ROOT_DIR/k8s/erctl.sh" pipeline start \
        -d "$CONFIG_DIR" \
        --data-locality "$strategy" \
        "${EXTRA_ARGS[@]}"
done
