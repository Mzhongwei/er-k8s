#!/usr/bin/env bash

# Script to manage the pipeline

if [ -z "${BASH_VERSION:-}" ]; then
    exec bash "$0" "$@"
fi

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

NAMESPACE="${EAER_PIPELINE_NAMESPACE:-argo}"
PIPELINE_MANIFEST="${ROOT_DIR}/k8s/argo/pipeline.yaml"
PVC_MANIFEST_DIR="${ROOT_DIR}/k8s/argo/pvc-manifests"
PV_MANIFEST_DIR="${ROOT_DIR}/k8s/argo/pv-manifests"
PIPELINE_CONFIGMAP="er-pipeline-config"
VERSION_NAMES_FILE="${SCRIPT_DIR}/pipeline-version-names.txt"

PVC_NAMES=(
    pipeline-bert-model-claim
    pipeline-buffer-data-claim
    pipeline-data-claim
    pipeline-decision-evaluation-cache-claim
    pipeline-embedding-model-cache-claim
    pipeline-feature-index-cache-claim
    pipeline-graph-cache-claim
    pipeline-reports-claim
)

usage() {
    cat << 'EOF'
Usage: erctl pipeline [start|stop|terminate] [options]
Manage the pipeline.
Options:
    start                    Start the pipeline (default)
    stop                     Stop the latest pipeline
    terminate                Terminate the latest pipeline and clean up resources
    -m, --mode MODE          ConfigMap mode for pipeline config (`embedding` or `bert`) default: `embedding`
    -c, --configmaps         Create the pipeline configmaps before submitting
    -d, --dataset            Sync the dataset into the pipeline PVC before submitting
    -r, --random-version-name    Pick a random version name from the local pool
    -h, --help, -help, help  Show this help
EOF
}

require_cmd() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Required command not found: $cmd"
        exit 1
    fi
}

log_step() {
    printf '[pipeline] %s\n' "$1"
}

warn_pending_pvcs() {
    local pvc=""
    local pvc_status=""

    for pvc in "${PVC_NAMES[@]}"; do
        pvc_status="$(kubectl get pvc -n "$NAMESPACE" "$pvc" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
        if [ -n "$pvc_status" ] && [ "$pvc_status" != "Bound" ]; then
            log_step "PVC $pvc is $pvc_status; continuing without waiting"
        fi
    done
}

choose_random_version_name() {
    local names_file="$1"
    local available_count=""
    local chosen_name=""
    local temp_file=""

    if [ ! -f "$names_file" ]; then
        echo "Version name pool not found: $names_file"
        exit 1
    fi

    available_count="$(grep -vc '^[[:space:]]*$' "$names_file" || true)"
    if [ "$available_count" -le 0 ]; then
        echo "No version names left in $names_file"
        exit 1
    fi

    chosen_name="$(grep -v '^[[:space:]]*$' "$names_file" | shuf -n 1)"
    temp_file="$(mktemp)"

    awk -v choice="$chosen_name" '
        BEGIN { removed = 0 }
        /^[[:space:]]*$/ { print; next }
        !removed && $0 == choice { removed = 1; next }
        { print }
        END { if (!removed) exit 1 }
    ' "$names_file" > "$temp_file"

    mv "$temp_file" "$names_file"
    printf '%s\n' "$chosen_name"
}

prepare_pipeline_config_file() {
    local source_file="$1"
    local version_name="$2"
    local temp_file=""

    temp_file="$(mktemp)"

    if grep -qE '^[[:space:]]*(version_name|versionName)[[:space:]]*:' "$source_file"; then
        awk -v version_name="$version_name" '
            BEGIN { replaced = 0 }
            /^[[:space:]]*(version_name|versionName)[[:space:]]*:/ && !replaced {
                sub(/:.*/, ": \"" version_name "\"")
                replaced = 1
            }
            { print }
            END {
                if (!replaced) {
                    print "version_name: \"" version_name "\""
                }
            }
        ' "$source_file" > "$temp_file"
    elif grep -qF '__VERSION_NAME__' "$source_file"; then
        sed "s/__VERSION_NAME__/${version_name//\/\\}/g" "$source_file" > "$temp_file"
    else
        cat "$source_file" > "$temp_file"
        printf '\nversion_name: "%s"\n' "$version_name" >> "$temp_file"
    fi

    printf '%s\n' "$temp_file"
}

create_pipeline_configmap() {
    local config_type="$1"
    local version_name="${EAER_PIPELINE_VERSION_NAME:-}"
    local config_file="${ROOT_DIR}/code/Energy-Aware-Entity-Resolution/config/examples/config-${config_type}.yaml"
    local config_file_to_use="$config_file"
    local temp_file=""

    if [ -n "$version_name" ]; then
        config_file_to_use="$(prepare_pipeline_config_file "$config_file" "$version_name")"
        temp_file="$config_file_to_use"
    fi

    kubectl create configmap "$PIPELINE_CONFIGMAP" \
        --from-file="config.yaml=${config_file_to_use}" \
        --dry-run=client -o yaml | \
        kubectl apply -n "$NAMESPACE" -f -

    if [ -n "$temp_file" ]; then
        rm -f "$temp_file"
    fi
}

delete_pipeline_configmaps() {
    local configmap_names=()
    local configmap_ref=""

    while IFS= read -r configmap_ref; do
        [ -n "$configmap_ref" ] || continue
        configmap_names+=("${configmap_ref#configmap/}")
    done < <(kubectl get cm -n "$NAMESPACE" -o name | grep '^configmap/eaer-' || true)

    if [ "${#configmap_names[@]}" -gt 0 ]; then
        kubectl delete cm -n "$NAMESPACE" "${configmap_names[@]}"
    fi

    kubectl delete cm -n "$NAMESPACE" "$PIPELINE_CONFIGMAP" --ignore-not-found=true
}

delete_pipeline_storage() {
    local include_dataset_pvc="${1:-false}"
    local pvc=""
    local pod=""
    local pod_pvc=""
    local pods_to_delete=()
    local target_pvcs=()
    local pv_path=""
    local pv_node=""
    local pv_manifest=""
    local cleanup_pod_name=""

    for pvc in "${PVC_NAMES[@]}"; do
        if [ "$pvc" = "pipeline-data-claim" ] && [ "$include_dataset_pvc" != true ]; then
            continue
        fi

        target_pvcs+=("$pvc")
    done

    # Delete any pod that still references one of the target PVCs.
    # This avoids PVCs getting stuck in Terminating due to lingering Completed pods.
    while IFS= read -r pod; do
        [ -n "$pod" ] || continue

        while IFS= read -r pod_pvc; do
            [ -n "$pod_pvc" ] || continue
            for pvc in "${target_pvcs[@]}"; do
                if [ "$pod_pvc" = "$pvc" ]; then
                    pods_to_delete+=("$pod")
                    break
                fi
            done
        done < <(kubectl get pod -n "$NAMESPACE" "$pod" -o jsonpath='{range .spec.volumes[*]}{.persistentVolumeClaim.claimName}{"\n"}{end}' 2>/dev/null || true)
    done < <(kubectl get pods -n "$NAMESPACE" --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null || true)

    if [ "${#pods_to_delete[@]}" -gt 0 ]; then
        mapfile -t pods_to_delete < <(printf '%s\n' "${pods_to_delete[@]}" | awk '!seen[$0]++')
        kubectl delete pod -n "$NAMESPACE" "${pods_to_delete[@]}" --ignore-not-found=true --wait=false
    fi

    for pvc in "${target_pvcs[@]}"; do
        case "$pvc" in
            pipeline-bert-model-claim)
                pv_path="/var/lib/eaer/pv/pipeline-bert-model-claim"
                pv_manifest="${PV_MANIFEST_DIR}/pv-pipeline-bert-model-claim.yaml"
                ;;
            pipeline-buffer-data-claim)
                pv_path="/var/lib/eaer/pv/pipeline-buffer-data-claim"
                pv_manifest="${PV_MANIFEST_DIR}/pv-pipeline-buffer-data-claim.yaml"
                ;;
            pipeline-data-claim)
                pv_path="/tmp/eaer/pv/pipeline-data-claim"
                pv_manifest="${PV_MANIFEST_DIR}/pv-pipeline-data-claim.yaml"
                ;;
            pipeline-decision-evaluation-cache-claim)
                pv_path="/var/lib/eaer/pv/pipeline-decision-evaluation-cache-claim"
                pv_manifest="${PV_MANIFEST_DIR}/pv-pipeline-decision-evaluation-cache-claim.yaml"
                ;;
            pipeline-embedding-model-cache-claim)
                pv_path="/var/lib/eaer/pv/pipeline-embedding-model-cache-claim"
                pv_manifest="${PV_MANIFEST_DIR}/pv-pipeline-embedding-model-cache-claim.yaml"
                ;;
            pipeline-feature-index-cache-claim)
                pv_path="/var/lib/eaer/pv/pipeline-feature-index-cache-claim"
                pv_manifest="${PV_MANIFEST_DIR}/pv-pipeline-feature-index-cache-claim.yaml"
                ;;
            pipeline-graph-cache-claim)
                pv_path="/var/lib/eaer/pv/pipeline-graph-cache-claim"
                pv_manifest="${PV_MANIFEST_DIR}/pv-pipeline-graph-cache-claim.yaml"
                ;;
            pipeline-reports-claim)
                pv_path="/var/lib/eaer/pv/pipeline-reports-claim"
                pv_manifest="${PV_MANIFEST_DIR}/pv-pipeline-reports-claim.yaml"
                ;;
            *)
                pv_path=""
                pv_manifest=""
                ;;
        esac

        if [ -n "$pv_path" ] && [ -n "$pv_manifest" ] && [ -f "$pv_manifest" ]; then
            pv_node="$(awk '
                $1 == "values:" { getline; gsub(/^[[:space:]]*-[[:space:]]*/, ""); print; exit }
            ' "$pv_manifest")"

            if [ -z "$pv_node" ]; then
                echo "Could not determine node selector from PV manifest: $pv_manifest"
                exit 1
            fi

            cleanup_pod_name="eaer-pv-cleanup-${pvc}"
            log_step "Clearing host path $pv_path"
            kubectl apply -n "$NAMESPACE" -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${cleanup_pod_name}
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: ${pv_node}
  containers:
    - name: cleaner
      image: busybox:1.36
      command:
        - sh
        - -c
        - rm -rf /mnt/*
      volumeMounts:
        - name: target
          mountPath: /mnt
  volumes:
    - name: target
      hostPath:
        path: ${pv_path}
        type: DirectoryOrCreate
EOF

            if ! kubectl wait -n "$NAMESPACE" --for=jsonpath='{.status.phase}'=Succeeded "pod/${cleanup_pod_name}" --timeout=120s; then
                kubectl logs -n "$NAMESPACE" "pod/${cleanup_pod_name}" || true
                kubectl delete pod -n "$NAMESPACE" "${cleanup_pod_name}" --ignore-not-found=true --wait=false
                exit 1
            fi

            kubectl delete pod -n "$NAMESPACE" "${cleanup_pod_name}" --ignore-not-found=true --wait=false
        fi
    done
}

latest_pipeline_workflow() {
    kubectl get wf -n "$NAMESPACE" --no-headers -o custom-columns=NAME:.metadata.name,CREATED:.metadata.creationTimestamp \
        | awk '$1 ~ /^pipeline-/ { print $1 "\t" $2 }' \
        | sort -k2,2 \
        | tail -n 1 \
        | cut -f1
}

start_pipeline() {
    local version_name=""

    if [ "$RANDOM_VERSION_NAME" = true ]; then
        version_name="$(choose_random_version_name "$VERSION_NAMES_FILE")"
        export EAER_PIPELINE_VERSION_NAME="$version_name"
        echo "Using pipeline version name: $version_name"
    fi

    log_step "Cleaning up previous pipeline storage"
    delete_pipeline_storage "$SYNC_DATASET"

    if [ ! -d "$PVC_MANIFEST_DIR" ]; then
        echo "PVC manifest directory not found: $PVC_MANIFEST_DIR"
        exit 1
    fi

    # If PV manifests exist, apply them first (PV objects are cluster-scoped)
    if [ -d "$PV_MANIFEST_DIR" ]; then
        log_step "Applying PV manifests from $PV_MANIFEST_DIR"
        kubectl apply -f "$PV_MANIFEST_DIR" || true
    fi

    log_step "Applying PVC manifests from $PVC_MANIFEST_DIR"
    kubectl apply -n "$NAMESPACE" -f "$PVC_MANIFEST_DIR"

    warn_pending_pvcs

    if [ "$SYNC_DATASET" = true ]; then
        log_step "Syncing dataset into PVC"
        bash "${SCRIPT_DIR}/erctl" dataset
    fi

    if [ "$CREATE_CONFIGMAPS" = true ]; then
        delete_pipeline_configmaps
        log_step "Creating ConfigMaps in $PIPELINE_MODE mode"
        bash "${SCRIPT_DIR}/erctl" configmaps "$PIPELINE_MODE"
    elif [ "$RANDOM_VERSION_NAME" = true ]; then
        log_step "Creating pipeline ConfigMap in $PIPELINE_MODE mode"
        create_pipeline_configmap "$PIPELINE_MODE"
    fi

    log_step "Submitting Argo workflow"
    argo submit -n "$NAMESPACE" "$PIPELINE_MANIFEST"
}

stop_pipeline() {
    local workflow_name

    workflow_name="$(latest_pipeline_workflow || true)"
    if [ -z "$workflow_name" ]; then
        echo "No pipeline workflow found in namespace $NAMESPACE."
        exit 0
    fi

    argo stop -n "$NAMESPACE" "$workflow_name"
}

terminate_pipeline() {
    local workflow_name

    workflow_name="$(latest_pipeline_workflow || true)"
    if [ -n "$workflow_name" ]; then
        argo terminate -n "$NAMESPACE" "$workflow_name"
    else
        echo "No pipeline workflow found in namespace $NAMESPACE."
    fi

    delete_pipeline_configmaps
    delete_pipeline_storage true
}

ACTION="start"
RANDOM_VERSION_NAME=false
CREATE_CONFIGMAPS=false
SYNC_DATASET=false
PIPELINE_MODE="embedding"

if [ $# -gt 0 ]; then
    case "$1" in
        start|stop|terminate)
            ACTION="$1"
            shift
            ;;
        -h|--help|-help|help)
            usage
            exit 0
            ;;
    esac
fi

if [ "$ACTION" = "start" ]; then
    while [ $# -gt 0 ]; do
        current_arg="$1"
        shift

        case "$current_arg" in
            -[!-]* )
                short_flags="${current_arg#-}"
                while [ -n "$short_flags" ]; do
                    short_flag="${short_flags%${short_flags#?}}"
                    short_flags="${short_flags#?}"

                    case "$short_flag" in
                        r)
                            RANDOM_VERSION_NAME=true
                            ;;
                        c)
                            CREATE_CONFIGMAPS=true
                            ;;
                        d)
                            SYNC_DATASET=true
                            ;;
                        m)
                            if [ -n "$short_flags" ]; then
                                echo "-m cannot be bundled with other short options. Use '-m MODE'."
                                exit 1
                            fi
                            if [ $# -lt 1 ]; then
                                echo "Missing value for -m. Expected 'embedding' or 'bert'."
                                exit 1
                            fi
                            PIPELINE_MODE="$1"
                            shift
                            ;;
                        *)
                            echo "Unknown option for pipeline start: -$short_flag"
                            echo "Use 'erctl pipeline --help' for usage information."
                            exit 1
                            ;;
                    esac
                done
                ;;
            -r|--random-version-name|--random-name)
                RANDOM_VERSION_NAME=true
                ;;
            -c|--configmaps)
                CREATE_CONFIGMAPS=true
                ;;
            -d|--dataset)
                SYNC_DATASET=true
                ;;
            -m|--mode)
                if [ $# -lt 1 ]; then
                    echo "Missing value for $1. Expected 'embedding' or 'bert'."
                    exit 1
                fi
                PIPELINE_MODE="$1"
                shift
                ;;
            --mode=*)
                PIPELINE_MODE="${1#*=}"
                ;;
            -h|--help|-help|help)
                usage
                exit 0
                ;;
            *)
                echo "Unknown option for pipeline start: $current_arg"
                echo "Use 'erctl pipeline --help' for usage information."
                exit 1
                ;;
        esac
    done

    if [[ "$PIPELINE_MODE" != "embedding" && "$PIPELINE_MODE" != "bert" ]]; then
        echo "Invalid mode: $PIPELINE_MODE. Use 'embedding' or 'bert'."
        exit 1
    fi

    require_cmd kubectl
    require_cmd argo
    require_cmd awk
    require_cmd shuf
    start_pipeline
elif [ "$ACTION" = "stop" ]; then
    if [ $# -gt 0 ]; then
        echo "Stop does not accept additional options."
        echo "Use 'erctl pipeline --help' for usage information."
        exit 1
    fi

    require_cmd kubectl
    require_cmd argo
    require_cmd awk
    stop_pipeline
elif [ "$ACTION" = "terminate" ]; then
    if [ $# -gt 0 ]; then
        echo "Terminate does not accept additional options."
        echo "Use 'erctl pipeline --help' for usage information."
        exit 1
    fi

    require_cmd kubectl
    require_cmd argo
    require_cmd awk
    terminate_pipeline
else
    echo "Unknown pipeline action: $ACTION"
    exit 1
fi

