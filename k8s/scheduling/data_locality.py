#!/usr/bin/env python3
"""Apply the fixed DL1-DL8 data-locality experiment matrix to compiled manifests."""
from __future__ import annotations

import argparse
import csv
import os
import tempfile
from pathlib import Path
from typing import Any

import yaml


SERVER2 = "server2-labo"
LAPTOP = "zhongwei-lap"
SERVER1 = "server1-k3s-worker"
HOSTNAME_KEY = "kubernetes.io/hostname"

TRANSFER_VOLUMES = {
    "pipeline-communication",
    "pipeline-buffer-data",
    "pipeline-decision-evaluation-cache",
}
LOCAL_VOLUME_DIRS = {
    "pipeline-data": "dataset",
    "pipeline-graph-cache": "graph",
    "pipeline-embedding-model-cache": "embedding",
    "pipeline-feature-index-cache": "index",
    "pipeline-communication": "communication",
    "pipeline-buffer-data": "buffers",
    "pipeline-decision-evaluation-cache": "predicted",
}
DATASET_PATH = "/srv/nfs/k8s/data"
LOCAL_RUN_ROOT = "/srv/nfs/k8s/eaer-local"

CANDIDATE_TASKS = {
    "embtrai-cg-feature-extraction",
    "embtrai-feature-index-construction",
    "cg-feature-extraction",
    "candidate-enumeration",
}
GRAPH_TASKS = {
    "embtrai-graph-construction",
    "embtrai-random-walk",
    "graph-construction",
    "random-walk",
}
EMBEDDING_TASKS = {
    "embtrai-embedding-training",
    "embedding-training",
    "calculating-similarity",
    "decision-making",
}
EXPECTED_BATCH_TASKS = {
    "embtrai-normalization",
    "embtrai-graph-construction",
    "embtrai-random-walk",
    "embtrai-cg-feature-extraction",
    "embtrai-embedding-training",
    "embtrai-feature-index-construction",
}
EXPECTED_INCREMENTAL_TASKS = {
    "calculating-similarity",
    "candidate-enumeration",
    "cg-feature-extraction",
    "decision-making",
    "embedding-training",
    "evaluation",
    "graph-construction",
    "kafka-consumer",
    "kafka-producer",
    "normalization",
    "random-walk",
}

PLAN_FIELDS = (
    "strategy", "phase", "task", "node", "volume", "storage_type",
    "physical_path", "reason",
)


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(
        description="Apply an absolute DL1-DL8 placement/storage strategy."
    )
    parser.add_argument("--strategy", required=True, choices=[f"DL{i}" for i in range(1, 9)])
    parser.add_argument("--run-token", default="manual", help="Unique local-directory token")
    parser.add_argument(
        "--batch", type=Path,
        default=root / "k8s/pipeline/exec/batch/pipeline.yaml",
    )
    parser.add_argument(
        "--incremental-dir", type=Path,
        default=root / "k8s/pipeline/exec/incremental/workers",
    )
    parser.add_argument(
        "--plan-output", type=Path,
        default=root / "k8s/pipeline/exec/data-locality-plan.tsv",
    )
    return parser.parse_args()


def target_node(strategy: str, task: str) -> tuple[str, str]:
    if strategy in {"DL1", "DL2"}:
        return SERVER2, "all tasks are fixed to server2"
    special_node = LAPTOP if strategy in {"DL3", "DL4", "DL5"} else SERVER1
    special_tasks = (
        CANDIDATE_TASKS if strategy in {"DL3", "DL6"}
        else GRAPH_TASKS if strategy in {"DL4", "DL7"}
        else EMBEDDING_TASKS
    )
    if task in special_tasks:
        return special_node, f"{strategy} selected task group"
    return SERVER2, f"{strategy} default task placement"


def exact_affinity(node: str) -> dict[str, Any]:
    return {
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{
                "matchExpressions": [{
                    "key": HOSTNAME_KEY,
                    "operator": "In",
                    "values": [node],
                }]
            }]
        }
    }


def apply_node(spec: dict[str, Any], node: str) -> None:
    affinity = spec.setdefault("affinity", {})
    if not isinstance(affinity, dict):
        raise ValueError("affinity must be a YAML object")
    affinity["nodeAffinity"] = exact_affinity(node)


def local_path(strategy: str, run_token: str, volume_name: str) -> str:
    if volume_name == "pipeline-data":
        return DATASET_PATH
    return f"{LOCAL_RUN_ROOT}/{run_token}/{LOCAL_VOLUME_DIRS[volume_name]}"


def storage_for(strategy: str, run_token: str, volume_name: str) -> tuple[str, str, str]:
    if volume_name not in LOCAL_VOLUME_DIRS:
        return "configuration", "", "ConfigMap or non-data volume"
    if strategy == "DL2" or (strategy not in {"DL1", "DL2"} and volume_name in TRANSFER_VOLUMES):
        return "nfs-pvc", "", "shared NFS transfer volume"
    path = local_path(strategy, run_token, volume_name)
    reason = "server2 local dataset" if volume_name == "pipeline-data" else "node-local experiment state"
    return "hostPath", path, reason


def transform_volumes(
    volumes: list[dict[str, Any]], strategy: str, run_token: str
) -> list[tuple[str, str, str, str]]:
    rows = []
    for volume in volumes:
        name = str(volume.get("name", ""))
        storage_type, path, reason = storage_for(strategy, run_token, name)
        if storage_type == "hostPath":
            volume.pop("persistentVolumeClaim", None)
            volume["hostPath"] = {
                "path": path,
                "type": "Directory" if name == "pipeline-data" else "DirectoryOrCreate",
            }
        rows.append((name, storage_type, path, reason))
    return rows


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Compiled manifest not found: {path}")
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Manifest must contain one YAML object: {path}")
    return value


def atomic_write_yaml(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            yaml.safe_dump(value, stream, sort_keys=False, allow_unicode=True)
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def transform_batch(
    path: Path, strategy: str, run_token: str, plan: list[dict[str, str]]
) -> set[str]:
    document = load_yaml(path)
    templates = document.get("spec", {}).get("templates", [])
    found: set[str] = set()
    for template in templates:
        task = str(template.get("name", ""))
        if task not in EXPECTED_BATCH_TASKS:
            continue
        found.add(task)
        node, placement_reason = target_node(strategy, task)
        apply_node(template, node)
        volume_rows = transform_volumes(template.get("volumes", []), strategy, run_token)
        if not volume_rows:
            volume_rows = [("", "none", "", "task has no data volume")]
        for volume, storage_type, physical_path, storage_reason in volume_rows:
            plan.append({
                "strategy": strategy, "phase": "batch", "task": task, "node": node,
                "volume": volume, "storage_type": storage_type,
                "physical_path": physical_path,
                "reason": f"{placement_reason}; {storage_reason}",
            })
    missing = EXPECTED_BATCH_TASKS - found
    if missing:
        raise ValueError(f"Missing expected batch templates: {', '.join(sorted(missing))}")
    atomic_write_yaml(path, document)
    return found


def transform_incremental(
    directory: Path, strategy: str, run_token: str, plan: list[dict[str, str]]
) -> set[str]:
    found: set[str] = set()
    for path in sorted(directory.glob("*.yaml")):
        document = load_yaml(path)
        task = str(document.get("metadata", {}).get("name", ""))
        if task not in EXPECTED_INCREMENTAL_TASKS:
            continue
        found.add(task)
        pod_spec = document["spec"]["template"]["spec"]
        node, placement_reason = target_node(strategy, task)
        apply_node(pod_spec, node)
        volume_rows = transform_volumes(pod_spec.get("volumes", []), strategy, run_token)
        if not volume_rows:
            volume_rows = [("", "none", "", "task has no data volume")]
        for volume, storage_type, physical_path, storage_reason in volume_rows:
            plan.append({
                "strategy": strategy, "phase": "incremental", "task": task, "node": node,
                "volume": volume, "storage_type": storage_type,
                "physical_path": physical_path,
                "reason": f"{placement_reason}; {storage_reason}",
            })
        atomic_write_yaml(path, document)
    missing = EXPECTED_INCREMENTAL_TASKS - found
    if missing:
        raise ValueError(f"Missing expected incremental jobs: {', '.join(sorted(missing))}")
    return found


def write_plan(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=PLAN_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    safe_token = args.run_token.strip()
    if not safe_token or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for char in safe_token):
        raise SystemExit("--run-token may contain only letters, digits, '.', '_' and '-'")
    plan: list[dict[str, str]] = []
    transform_batch(args.batch, args.strategy, safe_token, plan)
    transform_incremental(args.incremental_dir, args.strategy, safe_token, plan)
    write_plan(args.plan_output, plan)
    print(f"Data-locality strategy {args.strategy} applied ({len(plan)} task-volume rows).")
    print(f"Data-locality plan: {args.plan_output}")


if __name__ == "__main__":
    main()
