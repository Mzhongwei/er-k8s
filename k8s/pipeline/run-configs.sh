#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR=""
START_ARGS=()

usage() {
    cat << 'EOF'
Usage: erctl pipeline start -d <config-directory> [start options]

Run every *.yaml and *.yml file in the directory sequentially. All remaining options are
passed to `pipeline start` (including --data-locality DL1); execution stops when one
configuration fails.
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        -d|--directory)
            [ $# -ge 2 ] || { echo "Missing value for $1." >&2; exit 1; }
            CONFIG_DIR="$2"
            shift 2
            ;;
        --directory=*)
            CONFIG_DIR="${1#*=}"
            shift
            ;;
        -c|--config|--config=*)
            echo "-c/--config and -d/--directory cannot be used together." >&2
            exit 1
            ;;
        -h|--help|-help|help)
            usage
            exit 0
            ;;
        *)
            START_ARGS+=("$1")
            shift
            ;;
    esac
done

[ -n "$CONFIG_DIR" ] || { echo "Missing required -d/--directory <path>." >&2; exit 1; }
[ -d "$CONFIG_DIR" ] || { echo "Config directory not found: $CONFIG_DIR" >&2; exit 1; }
CONFIG_DIR="$(cd "$CONFIG_DIR" && pwd)"

mapfile -d '' CONFIGS < <(
    find "$CONFIG_DIR" -maxdepth 1 -type f \( -name '*.yaml' -o -name '*.yml' \) -print0 |
        sort -z
)
[ "${#CONFIGS[@]}" -gt 0 ] || { echo "No YAML config files found in: $CONFIG_DIR" >&2; exit 1; }

total="${#CONFIGS[@]}"
for index in "${!CONFIGS[@]}"; do
    config="${CONFIGS[$index]}"
    echo "[$((index + 1))/$total] Running $(basename "$config")"
    bash "$SCRIPT_DIR/pipeline.sh" start -c "$config" "${START_ARGS[@]}"
done
