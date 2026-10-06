#!/usr/bin/env python3
"""Motivation benchmark with two designs.

capacity (default): a short list of whole-pipeline execution configurations (BERT CPU cores,
BERT on the GPU, CPU parallelism of the random walk and Word2Vec, BERT on the laptop). They
differ in how fast they drain the stream, so each meets a different deadline. The stream
rate is the simulator's csv.file.timeout (services/dataStreamSimulator application.properties,
baked into the kafka-producer image); the design is sized for 50 ms per record, i.e. one
408-record window about every 20 s.

sweep: one-factor-at-a-time sweeps of random-walk processes, Gensim workers, and BERT
matching device/cores, each on two machines.

Offline preparation (BERT training and embedding training, i.e. the batch phase) runs once
per dataset at the highest configuration on the base node; its trained state is saved as a
seed and its energy is recorded in offline-prep.csv but not used by the analysis. Every
trial restores that seed and runs only the incremental phase. A trial keeps all incremental
stages at the baseline configuration on the base node, except the swept stage, which takes
the swept setting and is pinned to the swept node. Runs shared by several sweeps (the
all-baseline point) execute once per repetition. Energy is kept separately for EcoFLOC and
Alumet.
"""
from __future__ import annotations

import argparse
import ast
import csv
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "code/Energy-Aware-Entity-Resolution/config/examples/config-embedding.yaml"
SERVER2 = "server2-labo"
THINKPAD = "k3s-worker-thinkpad"
RESULTS_ROOT = ROOT / "k8s/results"
DATASETS = {
    "8-movie": {
        "data_source_A": "/data/rtas/8-movie/table_A.csv",
        "data_source_B": "/data/rtas/8-movie/table_B.csv",
        "ground_truth": "/data/rtas/8-movie/matches.txt",
        "trainset_path": "/data/rtas/8-movie/train.csv",
        "evalset_path": "/data/rtas/8-movie/eval.csv",
        "record_id_source_field": "id",
        "top_k": 1,
    },
    # "large1-pdc": {
    #     "data_source_A": "/data/large_datasets/large1-pdc/tableA.jsonl",
    #     "data_source_B": "/data/large_datasets/large1-pdc/tableB.jsonl",
    #     "ground_truth": "/data/large_datasets/large1-pdc/matches.txt",
    #     "top_k": 5,
    # },
}
# Swept values per stage and node. The thinkpad has fewer than 8 allocatable CPUs and no GPU.
# BERT values are "cuda" or "cpu:<cores>" (bert_matching.cpus: Pod CPUs and torch threads).
MATRIX: dict[str, dict[str, list[Any]]] = {
    "random-walk": {SERVER2: [1, 2, 4, 8], THINKPAD: [1, 2, 4]},
    "embedding": {SERVER2: [1, 2, 4, 8], THINKPAD: [1, 2, 4]},
    "bert": {SERVER2: ["cuda", "cpu:2", "cpu:4", "cpu:8"], THINKPAD: ["cpu:4"]},
}
# BERT CPU cores when a point does not choose them; equals the manifest's former CPU limit.
DEFAULT_BERT_CPUS = 2
# CPU requested by the incremental stages that are not configured here (similarity,
# candidate enumeration, graph construction, Kafka, ...), all on the base node.
OTHER_STAGES_CPUS = 2
# Scheduling task names pinned to the swept node. Only the incremental phase is swept: the
# batch phase is the shared offline preparation.
SWEEP_TASKS = {
    "random-walk": {"incremental": ["random-walk"]},
    "embedding": {"incremental": ["embedding-training"]},
    "bert": {"incremental": ["bert-matching"]},
}
ENERGY_PROVIDERS = ("ecofloc", "alumet")
POINT_FIELDS = (
    "random_walk_processes", "random_walk_node", "gensim_workers", "embedding_node",
    "bert_device", "bert_node", "bert_cpus",
)
ENERGY_COMPONENTS = ("cpu", "gpu", "ram", "storage", "nic", "other")
# Both providers are kept side by side; blank means that provider produced no summary.
# Alumet's total is node hardware energy; its workload-attributed share is kept as well.
ENERGY_SUMMARY_FIELDS = tuple(
    field
    for provider in ENERGY_PROVIDERS
    for field in (
        f"{provider}_status", f"{provider}_total_energy_j",
        *(f"{provider}_{component}_j" for component in ENERGY_COMPONENTS),
    )
) + ("alumet_workload_attributed_energy_j",)
RUN_FIELDS = (
    "dataset", "sweeps", *POINT_FIELDS, "repetition", "base_node", "seed_store",
    "status", "run_dir", "all_mutual_topk_f1", "left_target_only_f1",
    *ENERGY_SUMMARY_FIELDS, "trial_index", "started_at", "finished_at", "return_code",
)
PREP_FIELDS = (
    "dataset", "seed_store", *POINT_FIELDS, "bert_training_device", "base_node", "status",
    "run_dir", *ENERGY_SUMMARY_FIELDS, "started_at", "finished_at", "return_code",
)
STAGE_FIELDS = (
    "dataset", "sweeps", *POINT_FIELDS, "repetition", "run_dir", "phase", "task",
    "attempts", "windows", "elapsed_seconds", "pod_elapsed_seconds", "wait_seconds",
    "compute_seconds",
)
ENERGY_FIELDS = (
    "dataset", "sweeps", *POINT_FIELDS, "repetition", "base_node", "seed_store",
    "run_dir", "provider", "measurement_status", "phase", "task", "node", "cpu_j",
    "gpu_j", "ram_j", "storage_j", "nic_j", "other_j", "total_energy_j",
    "measured_components",
)


@dataclass(frozen=True)
class Point:
    random_walk_processes: int
    random_walk_node: str
    gensim_workers: int
    embedding_node: str
    bert_device: str
    bert_node: str
    bert_cpus: int = DEFAULT_BERT_CPUS  # used only when bert_device is "cpu"

    def as_row(self) -> dict[str, Any]:
        return {field: getattr(self, field) for field in POINT_FIELDS}

    def slug(self) -> str:
        short = {SERVER2: "s2", THINKPAD: "tp"}
        node = lambda name: short.get(name, re.sub(r"[^A-Za-z0-9]+", "", name))
        return (
            f"p{self.random_walk_processes}@{node(self.random_walk_node)}"
            f"-g{self.gensim_workers}@{node(self.embedding_node)}"
            f"-bert-{self.bert_device}{self.bert_cpus if self.bert_device == 'cpu' else ''}"
            f"@{node(self.bert_node)}"
        )

    def placement(self) -> dict[str, dict[str, str]]:
        pinned: dict[str, dict[str, str]] = {"batch": {}, "incremental": {}}
        for sweep, node in (
            ("random-walk", self.random_walk_node),
            ("embedding", self.embedding_node),
            ("bert", self.bert_node),
        ):
            for phase, tasks in SWEEP_TASKS[sweep].items():
                pinned[phase].update({task: node for task in tasks})
        return pinned


def point_from_row(row: dict[str, str]) -> Point | None:
    try:
        return Point(
            int(row["random_walk_processes"]), row["random_walk_node"],
            int(row["gensim_workers"]), row["embedding_node"],
            row["bert_device"], row["bert_node"],
            # Rows written before bert_cpus existed ran with the manifest's 2-core limit.
            int(row.get("bert_cpus") or DEFAULT_BERT_CPUS),
        )
    except (KeyError, TypeError, ValueError):
        return None


def build_points(
    baseline: Point, sweeps: list[str], nodes: list[str],
) -> dict[Point, list[str]]:
    """Map each distinct point to the sweeps it belongs to."""
    points: dict[Point, list[str]] = {}
    for sweep in sweeps:
        for node, values in MATRIX[sweep].items():
            if node not in nodes:
                continue
            for value in values:
                if sweep == "random-walk":
                    point = replace(baseline, random_walk_processes=value, random_walk_node=node)
                elif sweep == "embedding":
                    point = replace(baseline, gensim_workers=value, embedding_node=node)
                else:
                    device, _, cores = str(value).partition(":")
                    point = replace(baseline, bert_device=device, bert_node=node,
                                    bert_cpus=int(cores or baseline.bert_cpus))
                points.setdefault(point, []).append(sweep)
    return points


def capacity_points(base_node: str) -> dict[Point, list[str]]:
    """Whole-pipeline configurations of the capacity design, each with its label.

    Sized for one window about every 20 s (csv.file.timeout=50) with the round-2 algorithm
    settings: per window BERT needs ~70 s on 2 CPU cores (~2 s on the GPU) and Word2Vec
    ~21 s on one worker, ~14 s on two and ~10 s on four or more. A run lasts about
    windows x max(20 s, bottleneck time), so options far slower than the stream (BERT on 2
    or 4 cores) are left out. BERT on CPU is scaled until it nearly keeps up; with BERT on
    the GPU, Word2Vec becomes the bottleneck and one worker no longer keeps up, so the
    GPU group scales Word2Vec. The random walk (<1 s per window) stays at one process. The
    configured Pods leave >= 3 of the server's 20 cores for the other stages and system Pods.
    """
    s = base_node
    design = [
        ("bert-cpu8-w2v2", Point(1, s, 2, s, "cpu", s, 8)),
        ("bert-cpu12-w2v2", Point(1, s, 2, s, "cpu", s, 12)),
        ("bert-gpu-w2v1", Point(1, s, 1, s, "cuda", s)),
        ("bert-gpu-w2v2", Point(1, s, 2, s, "cuda", s)),
        ("bert-gpu-w2v4", Point(1, s, 4, s, "cuda", s)),
        ("bert-gpu-w2v8", Point(1, s, 8, s, "cuda", s)),
        ("bert-laptop-cpu6-w2v2", Point(1, s, 2, s, "cpu", THINKPAD, 6)),
    ]
    return {point: [label] for label, point in design}


def run(command: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(command), flush=True)
    return subprocess.run(command, cwd=ROOT, check=check, text=True, capture_output=capture)


def kubernetes_cpu(value: str) -> float:
    value = str(value).strip()
    return float(value[:-1]) / 1000.0 if value.endswith("m") else float(value)


def check_tools() -> None:
    missing = []
    if shutil.which("kubectl") is None:
        missing.append("kubectl is required")
    elif not run(["kubectl", "config", "current-context"], check=False, capture=True).stdout.strip():
        missing.append("kubectl has no current context")
    if shutil.which("argo") is None:
        missing.append("argo CLI is required")
    if missing:
        raise RuntimeError("; ".join(missing))


def node_capacity(node_name: str, require_gpu: bool) -> int:
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
            f"CUDA requested on {node_name}, but it does not advertise an allocatable GPU"
        )
    return max(1, math.floor(kubernetes_cpu(allocatable["cpu"])))


def validate_cluster(points: list[Point], base_node: str, bert_training_device: str) -> None:
    """Fail before any run if a point asks a node for more CPUs or a GPU it lacks."""
    check_tools()
    # Per node: the largest single Pod and the largest sum of the configured Pods in one
    # point (every stage of a point runs at the same time, so their CPUs must fit together).
    cpus: dict[str, int] = {}
    totals: dict[str, int] = {}
    gpu: dict[str, bool] = {base_node: bert_training_device == "cuda"}
    for point in points:
        point_total: dict[str, int] = {base_node: OTHER_STAGES_CPUS}
        for node, count in (
            (point.random_walk_node, point.random_walk_processes),
            (point.embedding_node, point.gensim_workers),
            (point.bert_node, point.bert_cpus if point.bert_device == "cpu" else 0),
        ):
            cpus[node] = max(cpus.get(node, 1), count)
            point_total[node] = point_total.get(node, 0) + count
        for node, total in point_total.items():
            totals[node] = max(totals.get(node, 0), total)
        gpu[point.bert_node] = gpu.get(point.bert_node, False) or point.bert_device == "cuda"
    for node in sorted(set(cpus) | set(gpu)):
        capacity = node_capacity(node, gpu.get(node, False))
        if cpus.get(node, 1) > capacity:
            raise RuntimeError(
                f"matrix requests {cpus[node]} CPU workers on {node}, but it has {capacity} CPUs"
            )
        if totals.get(node, 0) > capacity:
            raise RuntimeError(
                f"a point requests {totals[node]} CPUs at once on {node}, but it has {capacity} CPUs"
            )


def write_scheduling_config(output: Path, base_node: str, point: Point) -> Path:
    """Create a benchmark-local scheduling bundle: swept tasks on their node, the rest on base_node."""
    scheduling_dir = ROOT / "k8s/scheduling"
    placement_source = yaml.safe_load(
        (scheduling_dir / "temporary-placement.yaml").read_text(encoding="utf-8")
    )
    temporary = placement_source["temporary_placement"]
    temporary["enabled"] = True
    pinned = point.placement()
    for phase in ("batch", "incremental"):
        temporary[phase] = {
            task: pinned[phase].get(task, base_node) for task in temporary.get(phase, {})
        }

    bundle_dir = output / "scheduling"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    placement_path = bundle_dir / f"placement-{point.slug()}.yaml"
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
    entry_path = bundle_dir / f"scheduling-{point.slug()}.yaml"
    entry_path.write_text(
        yaml.safe_dump(entry_source, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    return entry_path


def seed_store_name(dataset: str) -> str:
    return f"serie1-{dataset}"


def write_config(
    base: dict[str, Any], destination: Path, dataset: str, point: Point,
    bert_training_device: str,
) -> None:
    config = dict(base)
    dataset_config = DATASETS[dataset]
    internal_keys = {"top_k", "record_id_source_field"}
    config.update({key: value for key, value in dataset_config.items() if key not in internal_keys})
    # Model, index and record paths include version_name, so it must match the seed's.
    config["version_name"] = f"bench-{dataset}"
    config["record_ids"] = dict(
        base.get("record_ids", {}),
        source_field=dataset_config.get("record_id_source_field", ""),
    )
    config["random_walk"] = dict(
        base.get("random_walk", {}), processes=point.random_walk_processes, seed=1729,
    )
    config["embeddings_training"] = dict(
        base.get("embeddings_training", {}), workers=point.gensim_workers, seed=1729,
    )
    config["bert_matching"] = dict(
        base.get("bert_matching", {}), enabled=True, device=point.bert_device,
    )
    if point.bert_device == "cpu":
        config["bert_matching"]["cpus"] = point.bert_cpus
    else:
        config["bert_matching"].pop("cpus", None)
    # BERT training only runs in the offline preparation, once per dataset.
    config["bert_training"] = dict(
        base.get("bert_training", {}) or {}, device=bert_training_device, seed=1729,
    )
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


def extract_energy(run_dir: Path) -> dict[str, Any]:
    """Whole-run energy from both providers, side by side, split by hardware component."""
    row: dict[str, Any] = {}
    for provider, components_key in (("ecofloc", "by_metric_j"), ("alumet", "hardware_by_device_j")):
        data = read_json(run_dir / "energy" / f"{provider}-summary.json")
        if not data:
            continue
        row[f"{provider}_status"] = data.get("measurement_status", "")
        row[f"{provider}_total_energy_j"] = data.get("total_energy_j", data.get("energy_j", ""))
        components: dict[str, float] = {}
        for name, raw in (data.get(components_key) or {}).items():
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if math.isfinite(value):
                component = normalize_energy_component(name)
                components[component] = components.get(component, 0.0) + value
        for component, value in components.items():
            row[f"{provider}_{component}_j"] = round(value, 6)
        if provider == "alumet":
            row["alumet_workload_attributed_energy_j"] = data.get("workload_attributed_energy_j", "")
    return row


def stage_rows(run_row: dict[str, Any], run_dir: Path) -> list[dict[str, Any]]:
    steps = read_json(run_dir / "step-metrics-summary.json").get("steps", {})
    identity = {field: run_row[field] for field in ("dataset", "sweeps", *POINT_FIELDS, "repetition", "run_dir")}
    rows = []
    for step in steps.values():
        rows.append({
            **identity,
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


def normalize_energy_component(name: str) -> str:
    """Map provider-specific metric names to stable benchmark CSV columns."""
    value = str(name).strip().lower()
    if "gpu" in value or "nvml" in value:
        return "gpu"
    if "dram" in value or "ram" in value or "memory" in value:
        return "ram"
    if "nic" in value or "network" in value:
        return "nic"
    if value in {"sd", "disk", "storage", "block_io"} or "storage" in value:
        return "storage"
    if "cpu" in value or "rapl" in value or "package" in value:
        return "cpu"
    return "other"


def incremental_placement(
    run_dir: Path,
) -> tuple[list[tuple[str, str]], dict[str, tuple[str, str]]]:
    """Return incremental (task, node) groups and Pod/UID identities for energy joins."""
    path = run_dir / "placement.tsv"
    if not path.exists():
        return [], {}
    with path.open(encoding="utf-8", newline="") as stream:
        placements = [
            row for row in csv.DictReader(stream, delimiter="\t")
            if row.get("phase") == "incremental" and row.get("task")
        ]
    groups = sorted({(row["task"], row.get("node", "")) for row in placements})
    identities: dict[str, tuple[str, str]] = {}
    for row in placements:
        group = row["task"], row.get("node", "")
        for identity in (row.get("pod"), row.get("pod_uid")):
            if identity:
                identities[identity] = group
    return groups, identities


def energy_row(
    run_row: dict[str, Any], provider: str, status: str, task: str, node: str,
    components: dict[str, float],
) -> dict[str, Any]:
    identity_fields = (
        "dataset", "sweeps", *POINT_FIELDS, "repetition", "base_node", "seed_store", "run_dir",
    )
    measured = sorted(components)
    return {
        **{field: run_row.get(field, "") for field in identity_fields},
        "provider": provider,
        "measurement_status": status,
        "phase": "incremental",
        "task": task,
        "node": node,
        **{f"{name}_j": components.get(name, "") for name in ENERGY_COMPONENTS},
        # Blank means unmeasured. It must remain distinguishable from a measured zero.
        "total_energy_j": round(sum(components.values()), 6) if measured else "",
        "measured_components": ",".join(measured),
    }


def provider_incremental_energy_rows(
    run_row: dict[str, Any], run_dir: Path, provider: str, summary: dict[str, Any],
    pod_hardware_key: str,
) -> list[dict[str, Any]]:
    groups, identities = incremental_placement(run_dir)
    grouped: dict[tuple[str, str], dict[str, float]] = {group: {} for group in groups}
    for identity, metrics in (summary.get(pod_hardware_key) or {}).items():
        group = identities.get(identity)
        if group is None or not isinstance(metrics, dict):
            continue
        target = grouped.setdefault(group, {})
        for raw_component, raw_value in metrics.items():
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value):
                continue
            component = normalize_energy_component(raw_component)
            target[component] = target.get(component, 0.0) + value
    status = str(summary.get("measurement_status", "unknown"))
    return [
        energy_row(run_row, provider, status, task, node, grouped[(task, node)])
        for task, node in sorted(grouped)
    ]


def incremental_energy_rows(run_row: dict[str, Any], run_dir: Path) -> list[dict[str, Any]]:
    """Flatten per-Pod energy into phase-aware incremental stage/hardware rows."""
    rows: list[dict[str, Any]] = []
    ecofloc = read_json(run_dir / "energy" / "ecofloc-summary.json")
    if ecofloc:
        rows.extend(provider_incremental_energy_rows(
            run_row, run_dir, "ecofloc", ecofloc, "by_pod_metric_j",
        ))
    alumet = read_json(run_dir / "energy" / "alumet-summary.json")
    if alumet:
        rows.extend(provider_incremental_energy_rows(
            run_row, run_dir, "alumet", alumet, "by_workload_pod_hardware_j",
        ))
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


def row_key(row: dict[str, Any]) -> tuple[Any, ...] | None:
    point = point_from_row(row)
    try:
        return (row["dataset"], point, int(row["repetition"])) if point else None
    except (KeyError, TypeError, ValueError):
        return None


def start_pipeline(
    config_path: Path, scheduling_config: Path, energy_monitor: str, phase: str, seed_store: str,
) -> tuple[subprocess.CompletedProcess[str], Path | None, str, str]:
    before = {path.resolve() for path in RESULTS_ROOT.glob("embedding-training-inference-evaluation-*")}
    started = datetime.now(timezone.utc).isoformat()
    result = run([
        "bash", "k8s/erctl.sh", "pipeline", "start", "-c", str(config_path),
        "--scheduling-config", str(scheduling_config),
        "--phase", phase, "--seed-store", seed_store,
        "--energy-monitor", energy_monitor, "--results-summary",
    ], check=False)
    finished = datetime.now(timezone.utc).isoformat()
    after = {path.resolve() for path in RESULTS_ROOT.glob("embedding-training-inference-evaluation-*")}
    created = sorted(after - before, key=lambda path: path.stat().st_mtime)
    return result, (created[-1] if created else None), started, finished


def split_list(raw: str) -> list[str]:
    return [value.strip() for value in raw.split(",") if value.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--design", choices=("capacity", "sweep"), default="capacity",
        help="capacity: whole-pipeline configurations (capacity_points); sweep: one factor at a time",
    )
    parser.add_argument("--datasets", default=",".join(DATASETS), help="Comma-separated dataset names")
    parser.add_argument("--sweeps", default=",".join(MATRIX),
                        help="sweep design only: comma-separated subset of: " + ",".join(MATRIX))
    parser.add_argument(
        "--nodes", default=f"{SERVER2},{THINKPAD}",
        help="Comma-separated nodes whose matrix entries are run",
    )
    parser.add_argument("--base-node", default=SERVER2, help="Node for every stage that is not swept")
    parser.add_argument("--baseline-processes", type=int, default=1, help="Random-walk processes when not swept")
    parser.add_argument("--baseline-workers", type=int, default=1, help="Gensim workers when not swept")
    parser.add_argument(
        "--baseline-bert-device", choices=("cpu", "cuda"), default="cpu",
        help="BERT matching device when not swept",
    )
    parser.add_argument(
        "--bert-training-device", choices=("cpu", "cuda"), default="cuda",
        help="Offline BERT training device, fixed on the base node for every run",
    )
    parser.add_argument(
        "--repetitions", type=int, default=1,
        help="Recorded runs per point (default 1: every configuration runs once)",
    )
    parser.add_argument(
        "--shuffle-seed", type=int, default=None,
        help="Randomize trial order with this seed; the order is saved to trial-order.json",
    )
    parser.add_argument("--warmup", action="store_true", help="Run the baseline once, unrecorded, first")
    parser.add_argument("--energy-monitor", choices=("ecofloc", "alumet", "ecofloc-alumet"), default="ecofloc-alumet")
    parser.add_argument("--output", type=Path, default=None,
                        help="default: reports/serie1-capacity or reports/serie1-motivation")
    parser.add_argument("--dry-run", action="store_true", help="Generate configs without starting workloads")
    parser.add_argument("--keep-going", action="store_true", help="Continue after a failed experiment")
    parser.add_argument("--no-resume", action="store_true", help="Rerun trials already marked Succeeded")
    parser.add_argument(
        "--rebuild-seeds", action="store_true",
        help="Rerun the offline preparation even if offline-prep.csv records a successful one",
    )
    args = parser.parse_args()

    datasets = split_list(args.datasets)
    unknown = sorted(set(datasets) - set(DATASETS))
    if unknown:
        parser.error(f"unknown datasets: {', '.join(unknown)}")
    sweeps = split_list(args.sweeps)
    if not sweeps or set(sweeps) - set(MATRIX):
        parser.error(f"--sweeps accepts only: {', '.join(MATRIX)}")
    nodes = split_list(args.nodes)
    if args.repetitions < 1 or args.baseline_processes < 1 or args.baseline_workers < 1:
        parser.error("--repetitions, --baseline-processes and --baseline-workers must be positive")

    baseline = Point(
        args.baseline_processes, args.base_node, args.baseline_workers, args.base_node,
        args.baseline_bert_device, args.base_node,
    )
    points = capacity_points(args.base_node) if args.design == "capacity" \
        else build_points(baseline, sweeps, nodes)
    if not points:
        parser.error("the selected sweeps and nodes produce an empty matrix")
    # Offline preparation: the highest matrix setting on the base node for every stage.
    base_values = lambda sweep, default: MATRIX[sweep].get(args.base_node) or [default]
    prep_point = Point(
        max(base_values("random-walk", args.baseline_processes)), args.base_node,
        max(base_values("embedding", args.baseline_workers)), args.base_node,
        "cuda" if "cuda" in base_values("bert", args.baseline_bert_device) else args.baseline_bert_device,
        args.base_node,
    )
    if not args.dry_run:
        validate_cluster([*points, baseline, prep_point], args.base_node, args.bert_training_device)
    base = yaml.safe_load(args.base_config.read_text(encoding="utf-8"))
    if not isinstance(base, dict):
        raise RuntimeError("base config root must be a YAML object")

    default_output = "reports/serie1-capacity" if args.design == "capacity" else "reports/serie1-motivation"
    output = (args.output or ROOT / default_output).resolve()
    configs_dir = output / "configs"
    prep_path = output / "offline-prep.csv"
    runs_path = output / "runs.csv"
    stages_path = output / "stage-timings.csv"
    energy_path = output / "incremental-stage-energy.csv"
    rows = load_rows(runs_path)
    prep_rows = load_rows(prep_path)
    stage_data = load_rows(stages_path)
    energy_data = load_rows(energy_path)
    # Make the new energy table useful when resuming an older benchmark: backfill every
    # retained run whose result directory is still available.
    for existing in rows:
        raw_run_dir = existing.get("run_dir", "")
        if not raw_run_dir:
            continue
        existing_run_dir = Path(raw_run_dir)
        if not existing_run_dir.is_absolute():
            existing_run_dir = ROOT / existing_run_dir
        if not existing_run_dir.is_dir():
            continue
        existing_key = row_key(existing)
        energy_data = [old for old in energy_data if row_key(old) != existing_key]
        energy_data.extend(incremental_energy_rows(existing, existing_run_dir))
        existing.update(extract_energy(existing_run_dir))
    completed = {row_key(row) for row in rows if row.get("status") == "Succeeded"}

    trials = [
        (dataset, point, repetition)
        for dataset in datasets
        for repetition in range(1, args.repetitions + 1)
        for point in points
    ]
    if args.shuffle_seed is not None:
        random.Random(args.shuffle_seed).shuffle(trials)
    output.mkdir(parents=True, exist_ok=True)
    (output / "trial-order.json").write_text(json.dumps({
        "shuffle_seed": args.shuffle_seed,
        "trials": [
            {"dataset": dataset, "repetition": repetition, "sweeps": points[point], **point.as_row()}
            for dataset, point, repetition in trials
        ],
    }, indent=2), encoding="utf-8")

    print(f"Experiment matrix ({args.design}): {len(points)} points x {args.repetitions} repetitions x "
          f"{len(datasets)} datasets = {len(trials)} runs")
    for point, members in points.items():
        print(f"  {point.slug():40s} sweeps={'+'.join(members)}")

    def prepare(dataset: str, point: Point) -> tuple[Path, Path]:
        config_path = configs_dir / f"{dataset}-{point.slug()}.yaml"
        write_config(base, config_path, dataset, point, args.bert_training_device)
        return config_path, write_scheduling_config(output, args.base_node, point)

    def run_offline_prep(dataset: str) -> int:
        """Train once per dataset at the highest configuration and save the seed."""
        seed_store = seed_store_name(dataset)
        done = any(
            row.get("dataset") == dataset and row.get("seed_store") == seed_store
            and row.get("status") == "Succeeded" for row in prep_rows
        )
        if done and not args.rebuild_seeds:
            print(f"[prep] {dataset} seed {seed_store} already built")
            return 0
        config_path = configs_dir / f"{dataset}-offline-prep.yaml"
        write_config(base, config_path, dataset, prep_point, args.bert_training_device)
        scheduling_config = write_scheduling_config(output, args.base_node, prep_point)
        print(f"[prep] {dataset} seed {seed_store} with {prep_point.slug()}", flush=True)
        if args.dry_run:
            return 0
        result, run_dir, started, finished = start_pipeline(
            config_path, scheduling_config, args.energy_monitor, "batch", seed_store,
        )
        manifest = read_json(run_dir / "manifest.json") if run_dir else {}
        prep_rows[:] = [row for row in prep_rows if row.get("dataset") != dataset]
        prep_rows.append({
            "dataset": dataset, "seed_store": seed_store, **prep_point.as_row(),
            "bert_training_device": args.bert_training_device, "base_node": args.base_node,
            "status": str(manifest.get("status") or ("Succeeded" if result.returncode == 0 else "Failed")),
            "run_dir": str(run_dir.relative_to(ROOT)) if run_dir else "",
            **(extract_energy(run_dir) if run_dir else {}),
            "started_at": started, "finished_at": finished, "return_code": result.returncode,
        })
        write_csv(prep_path, PREP_FIELDS, prep_rows)
        return result.returncode

    for dataset in datasets:
        code = run_offline_prep(dataset)
        if code != 0:
            print(f"Offline preparation failed for {dataset}; see {prep_path}", file=sys.stderr)
            return code

    if args.warmup and not args.dry_run:
        print("Warm-up run (not recorded)", flush=True)
        config_path, scheduling_config = prepare(datasets[0], baseline)
        start_pipeline(
            config_path, scheduling_config, args.energy_monitor, "incremental",
            seed_store_name(datasets[0]),
        )

    for index, (dataset, point, repetition) in enumerate(trials, start=1):
        key = dataset, point, repetition
        label = f"[{index}/{len(trials)}] {dataset} {point.slug()} rep={repetition}"
        config_path, scheduling_config = prepare(dataset, point)
        if not args.no_resume and key in completed:
            print(f"{label} skip completed")
            continue
        print(label, flush=True)
        if args.dry_run:
            continue

        seed_store = seed_store_name(dataset)
        result, run_dir, started, finished = start_pipeline(
            config_path, scheduling_config, args.energy_monitor, "incremental", seed_store,
        )
        manifest = read_json(run_dir / "manifest.json") if run_dir else {}
        status = str(manifest.get("status") or ("Succeeded" if result.returncode == 0 else "Failed"))
        all_f1, left_f1 = extract_f1(run_dir) if run_dir else ("", "")
        row: dict[str, Any] = {
            "dataset": dataset, "sweeps": "+".join(points[point]), **point.as_row(),
            "repetition": repetition, "base_node": args.base_node, "seed_store": seed_store,
            "status": status, "run_dir": str(run_dir.relative_to(ROOT)) if run_dir else "",
            "all_mutual_topk_f1": all_f1, "left_target_only_f1": left_f1,
            **(extract_energy(run_dir) if run_dir else {}), "trial_index": index,
            "started_at": started, "finished_at": finished, "return_code": result.returncode,
        }
        rows = [old for old in rows if row_key(old) != key]
        rows.append(row)
        stage_data = [old for old in stage_data if row_key(old) != key]
        energy_data = [old for old in energy_data if row_key(old) != key]
        if run_dir:
            stage_data.extend(stage_rows(row, run_dir))
            energy_data.extend(incremental_energy_rows(row, run_dir))
        write_csv(runs_path, RUN_FIELDS, rows)
        write_csv(stages_path, STAGE_FIELDS, stage_data)
        write_csv(energy_path, ENERGY_FIELDS, energy_data)
        if result.returncode != 0 and not args.keep_going:
            print(f"Experiment failed; resume later with the same command. Summary: {runs_path}", file=sys.stderr)
            return result.returncode

    write_csv(runs_path, RUN_FIELDS, rows)
    write_csv(stages_path, STAGE_FIELDS, stage_data)
    write_csv(energy_path, ENERGY_FIELDS, energy_data)
    print(f"Offline preparation (recorded, not analyzed): {prep_path}")
    print(f"Run summary: {runs_path}")
    print(f"Per-stage timings: {stages_path}")
    print(f"Incremental per-stage hardware energy: {energy_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
