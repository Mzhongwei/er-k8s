#!/usr/bin/env python3
"""Run the random-walk parallelism benchmark with CPU-only Gensim embeddings."""
from __future__ import annotations

import argparse
import ast
import csv
import json
import math
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "code/Energy-Aware-Entity-Resolution/config/examples/config-embedding.yaml"
DEFAULT_NODE = "server2-labo"
RESULTS_ROOT = ROOT / "k8s/results"
DATASETS = {
    "2-fordors_zagats": {
        "data_source_A": "/data/exp_datasets/2-fordors_zagats/tableA.csv",
        "data_source_B": "/data/exp_datasets/2-fordors_zagats/tableB.csv",
        "ground_truth": "/data/exp_datasets/2-fordors_zagats/matches.txt",
        "top_k": 1,
    },
    # "8-movie": {
    #     "data_source_A": "/data/exp_datasets/8-movie/tableA.csv",
    #     "data_source_B": "/data/exp_datasets/8-movie/tableB.csv",
    #     "ground_truth": "/data/exp_datasets/8-movie/matches.txt",
    #     "top_k": 1,
    # },
    # "large1-pdc": {
    #     "data_source_A": "/data/large_datasets/large1-pdc/tableA.jsonl",
    #     "data_source_B": "/data/large_datasets/large1-pdc/tableB.jsonl",
    #     "ground_truth": "/data/large_datasets/large1-pdc/matches.txt",
    #     "top_k": 5,
    # },
}
RUN_FIELDS = (
    "dataset", "random_walk_processes", "embedding_device", "execution_node", "status", "run_dir",
    "all_mutual_topk_f1", "left_target_only_f1", "energy_provider", "total_energy_j",
    "started_at", "finished_at", "return_code",
)
STAGE_FIELDS = (
    "dataset", "random_walk_processes", "embedding_device", "execution_node", "run_dir", "phase", "task",
    "attempts", "windows", "elapsed_seconds", "pod_elapsed_seconds", "wait_seconds",
    "compute_seconds",
)


def run(command: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(command), flush=True)
    return subprocess.run(command, cwd=ROOT, check=check, text=True, capture_output=capture)


def kubernetes_cpu(value: str) -> float:
    value = str(value).strip()
    return float(value[:-1]) / 1000.0 if value.endswith("m") else float(value)


def cluster_capacity(require_gpu: bool, node_name: str) -> int:
    missing = []
    if shutil.which("kubectl") is None:
        missing.append("kubectl is required")
        context = ""
    else:
        context = run(["kubectl", "config", "current-context"], check=False, capture=True).stdout.strip()
        if not context:
            missing.append("kubectl has no current context")
    if shutil.which("argo") is None:
        missing.append("argo CLI is required")
    if missing:
        raise RuntimeError("; ".join(missing))
    result = run(["kubectl", "get", "node", node_name, "-o", "json"], check=False, capture=True)
    if result.returncode != 0:
        raise RuntimeError(f"benchmark node not found: {node_name}")
    node = json.loads(result.stdout)
    conditions = node.get("status", {}).get("conditions", [])
    ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)
    if not ready:
        raise RuntimeError(f"benchmark node is not Ready: {node_name}")
    if node.get("spec", {}).get("unschedulable", False):
        raise RuntimeError(f"benchmark node is cordoned: {node_name}")
    allocatable = node.get("status", {}).get("allocatable", {})
    if require_gpu and int(float(allocatable.get("nvidia.com/gpu", 0))) < 1:
        raise RuntimeError(
            f"GPU experiments requested, but {node_name} does not advertise an allocatable GPU"
        )
    return max(1, math.floor(kubernetes_cpu(allocatable["cpu"])))


def write_fixed_scheduling_config(output: Path, node_name: str) -> Path:
    """Create a benchmark-local scheduling bundle that pins every task to one node."""
    scheduling_dir = ROOT / "k8s/scheduling"
    placement_source = yaml.safe_load(
        (scheduling_dir / "temporary-placement.yaml").read_text(encoding="utf-8")
    )
    temporary = placement_source["temporary_placement"]
    temporary["enabled"] = True
    for phase in ("batch", "incremental"):
        temporary[phase] = {task: node_name for task in temporary.get(phase, {})}

    bundle_dir = output / "scheduling"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    safe_node = re.sub(r"[^A-Za-z0-9_.-]+", "-", node_name)
    placement_path = bundle_dir / f"placement-{safe_node}.yaml"
    placement_path.write_text(
        yaml.safe_dump(placement_source, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )

    entry_source = yaml.safe_load(
        (scheduling_dir / "scheduling.yaml").read_text(encoding="utf-8")
    )
    includes = entry_source["includes"]
    for key, value in list(includes.items()):
        includes[key] = str((scheduling_dir / value).resolve())
    includes["placement"] = str(placement_path.resolve())
    entry_path = bundle_dir / f"scheduling-{safe_node}.yaml"
    entry_path.write_text(
        yaml.safe_dump(entry_source, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    return entry_path


def power_counts(maximum: int) -> list[int]:
    values, value = [], 1
    while value <= maximum:
        values.append(value)
        value *= 2
    return values


def parse_counts(raw: str, maximum: int) -> list[int]:
    if raw == "auto":
        return power_counts(maximum)
    values = sorted({int(item.strip()) for item in raw.split(",") if item.strip()})
    if not values or values[0] < 1:
        raise ValueError("--cpu-counts must contain positive integers")
    if values[-1] > maximum:
        raise ValueError(f"requested {values[-1]} CPUs, but the largest Ready node has {maximum}")
    return values


def write_config(base: dict[str, Any], destination: Path, dataset: str, processes: int, device: str) -> None:
    config = dict(base)
    dataset_config = DATASETS[dataset]
    config.update({key: value for key, value in dataset_config.items() if key != "top_k"})
    config["version_name"] = f"bench-{dataset}-p{processes}-{device}"
    config["random_walk"] = dict(base.get("random_walk", {}), processes=processes, seed=1729)
    # Keep the historical device column/config suffix so existing benchmark CSVs remain
    # readable. Gensim is CPU-only; workers comes from the base config.
    config["embeddings_training"] = dict(base.get("embeddings_training", {}), seed=1729)
    config["decision_making"] = dict(
        base.get("decision_making", {}), top_k=int(dataset_config["top_k"])
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(yaml.safe_dump(config, sort_keys=False, allow_unicode=True), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def extract_f1(run_dir: Path) -> tuple[Any, Any]:
    reports = list((run_dir / "matching").rglob("evaluation_report.json"))
    if reports:
        report = read_json(reports[-1])
        return (
            report.get("all_mutual_topk", {}).get("f1_score", ""),
            report.get("left_target_only", {}).get("f1_score", ""),
        )
    try:
        line = (run_dir / "matching-result.txt").read_text(encoding="utf-8")
        all_match = re.search(r"all_mutual_topk:\s*(\{.*?\}),\s*left_target_only", line)
        left_match = re.search(r"left_target_only:\s*(\{.*?\})\s*$", line)
        all_f1 = ast.literal_eval(all_match.group(1)).get("f1_score", "") if all_match else ""
        left_f1 = ast.literal_eval(left_match.group(1)).get("f1_score", "") if left_match else ""
        return all_f1, left_f1
    except (FileNotFoundError, SyntaxError, ValueError):
        return "", ""


def extract_energy(run_dir: Path) -> tuple[str, Any]:
    for provider in ("ecofloc", "alumet"):
        data = read_json(run_dir / "energy" / f"{provider}-summary.json")
        if data:
            value = data.get("total_energy_j", data.get("energy_j", ""))
            return provider, value
    return "", ""


def stage_rows(run_row: dict[str, Any], run_dir: Path) -> list[dict[str, Any]]:
    steps = read_json(run_dir / "step-metrics-summary.json").get("steps", {})
    rows = []
    for step in steps.values():
        rows.append({
            "dataset": run_row["dataset"],
            "random_walk_processes": run_row["random_walk_processes"],
            "embedding_device": run_row["embedding_device"],
            "execution_node": run_row["execution_node"],
            "run_dir": run_row["run_dir"],
            "phase": step.get("phase", ""),
            "task": step.get("task", ""),
            "attempts": step.get("attempts", ""),
            "windows": step.get("windows", ""),
            "elapsed_seconds": step.get("cumulative_elapsed_seconds", ""),
            "pod_elapsed_seconds": step.get("cumulative_pod_elapsed_seconds", ""),
            "wait_seconds": step.get("cumulative_wait_seconds", ""),
            "compute_seconds": step.get("cumulative_compute_seconds", ""),
        })
    return rows


def write_csv(path: Path, fields: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in fields} for row in rows)
    os.replace(temporary, path)


def load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--cpu-counts", default="auto", help="Comma list such as 1,2,4,8, or auto")
    parser.add_argument("--datasets", default=",".join(DATASETS), help="Comma-separated dataset names")
    parser.add_argument(
        "--devices", default="cpu",
        help="Compatibility option; the Gensim embedding implementation accepts only cpu",
    )
    parser.add_argument("--node", default=DEFAULT_NODE, help="Kubernetes node used by every task")
    parser.add_argument("--energy-monitor", choices=("ecofloc", "alumet", "ecofloc-alumet"), default="ecofloc-alumet")
    parser.add_argument("--output", type=Path, default=ROOT / "reports/random-walk-gpu-benchmark")
    parser.add_argument("--dry-run", action="store_true", help="Generate configs without starting workloads")
    parser.add_argument("--keep-going", action="store_true", help="Continue after a failed experiment")
    parser.add_argument("--no-resume", action="store_true", help="Rerun combinations already marked Succeeded")
    args = parser.parse_args()

    datasets = [value.strip() for value in args.datasets.split(",") if value.strip()]
    unknown = sorted(set(datasets) - set(DATASETS))
    if unknown:
        parser.error(f"unknown datasets: {', '.join(unknown)}")
    devices = [value.strip().lower() for value in args.devices.split(",") if value.strip()]
    if devices != ["cpu"]:
        parser.error("Gensim embeddings are CPU-only; --devices must be cpu")

    if args.dry_run:
        maximum = os.cpu_count() or 1
    else:
        maximum = cluster_capacity(False, args.node)
    counts = parse_counts(args.cpu_counts, maximum)
    base = yaml.safe_load(args.base_config.read_text(encoding="utf-8"))
    if not isinstance(base, dict):
        raise RuntimeError("base config root must be a YAML object")

    output = args.output.resolve()
    scheduling_config = write_fixed_scheduling_config(output, args.node)
    configs_dir = output / "configs"
    runs_path, stages_path = output / "runs.csv", output / "stage-timings.csv"
    rows = load_rows(runs_path)
    completed = {
        (row["dataset"], int(row["random_walk_processes"]), row["embedding_device"], row.get("execution_node", ""))
        for row in rows if row.get("status") == "Succeeded"
    }
    stage_data = load_rows(stages_path)

    combinations = [(dataset, count, device) for dataset in datasets for device in devices for count in counts]
    print(f"Experiment matrix: {len(combinations)} runs; CPU counts={counts}; devices={devices}")
    for index, (dataset, count, device) in enumerate(combinations, start=1):
        key = dataset, count, device, args.node
        config_path = configs_dir / f"{dataset}-p{count}-{device}.yaml"
        write_config(base, config_path, dataset, count, device)
        if not args.no_resume and key in completed:
            print(f"[{index}/{len(combinations)}] skip completed {dataset} p={count} {device}")
            continue
        print(f"[{index}/{len(combinations)}] {dataset} p={count} {device}", flush=True)
        if args.dry_run:
            continue

        before = {path.resolve() for path in RESULTS_ROOT.glob("embedding-training-inference-evaluation-*")}
        started = datetime.now(timezone.utc).isoformat()
        result = run([
            "bash", "k8s/erctl.sh", "pipeline", "start", "-c", str(config_path),
            "--scheduling-config", str(scheduling_config),
            "--energy-monitor", args.energy_monitor, "--results-summary",
        ], check=False)
        finished = datetime.now(timezone.utc).isoformat()
        after = {path.resolve() for path in RESULTS_ROOT.glob("embedding-training-inference-evaluation-*")}
        created = sorted(after - before, key=lambda path: path.stat().st_mtime)
        run_dir = created[-1] if created else None
        manifest = read_json(run_dir / "manifest.json") if run_dir else {}
        status = str(manifest.get("status") or ("Succeeded" if result.returncode == 0 else "Failed"))
        all_f1, left_f1 = extract_f1(run_dir) if run_dir else ("", "")
        provider, energy = extract_energy(run_dir) if run_dir else ("", "")
        row: dict[str, Any] = {
            "dataset": dataset, "random_walk_processes": count, "embedding_device": device,
            "execution_node": args.node,
            "status": status, "run_dir": str(run_dir.relative_to(ROOT)) if run_dir else "",
            "all_mutual_topk_f1": all_f1, "left_target_only_f1": left_f1,
            "energy_provider": provider, "total_energy_j": energy,
            "started_at": started, "finished_at": finished, "return_code": result.returncode,
        }
        rows = [old for old in rows if (
            old.get("dataset"), int(old.get("random_walk_processes", 0)), old.get("embedding_device"),
            old.get("execution_node", "")
        ) != key]
        rows.append(row)
        stage_data = [old for old in stage_data if (
            old.get("dataset"), int(old.get("random_walk_processes", 0)), old.get("embedding_device"),
            old.get("execution_node", "")
        ) != key]
        if run_dir:
            stage_data.extend(stage_rows(row, run_dir))
        write_csv(runs_path, RUN_FIELDS, rows)
        write_csv(stages_path, STAGE_FIELDS, stage_data)
        if result.returncode != 0 and not args.keep_going:
            print(f"Experiment failed; resume later with the same command. Summary: {runs_path}", file=sys.stderr)
            return result.returncode

    write_csv(runs_path, RUN_FIELDS, rows)
    write_csv(stages_path, STAGE_FIELDS, stage_data)
    print(f"Run summary: {runs_path}")
    print(f"Per-stage timings: {stages_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
