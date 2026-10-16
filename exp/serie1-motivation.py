#!/usr/bin/env python3
"""Motivation benchmark with three designs.

single (default): one configuration per dataset (the fastest: BERT on the GPU, eight Gensim
workers), run once, to get every dataset through the pipeline quickly. Each dataset sets its
own window size (DATASETS[...]["window_count"]); --record-interval-ms shortens the stream.

capacity: a short list of whole-pipeline execution configurations (BERT CPU cores,
BERT on the GPU, CPU parallelism of the random walk and Word2Vec, the laptop's own set). They
differ in how fast they drain the stream, so each meets a different deadline. The stream
rate is the simulator's csv.file.timeout (services/dataStreamSimulator application.properties,
baked into the kafka-producer image); the design is sized for 50 ms per record, i.e. one
408-record window about every 20 s.

sweep: one-factor-at-a-time sweeps of random-walk processes, Gensim workers, and BERT
matching device/cores.

Every stage of a run executes on one node. --nodes selects server2, thinkpad, or both, and
each selected node runs the same tests with its settings (DEFAULT_NODE_CONFIG updated by
NODE_OVERRIDES). Offline preparation (BERT training and embedding training, i.e. the batch
phase) runs once per dataset and node at that node's highest configuration; its trained state
is saved as a per-node seed and its energy is recorded in offline-prep.csv but not used by the
analysis. Every trial restores that seed and runs only the incremental phase. A sweep trial
keeps all incremental stages at the baseline configuration, except the swept stage, which
takes the swept setting. Runs shared by several sweeps (the all-baseline point) execute once
per repetition. Energy is kept separately for EcoFLOC and Alumet.
"""
from __future__ import annotations

import argparse
import ast
import atexit
import importlib.util
import csv
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "code/Energy-Aware-Entity-Resolution/config/examples/config-embedding.yaml"
SERVER2 = "server2-labo"
SERVER1 = "server1-k3s-worker"
THINKPAD = "k3s-worker-thinkpad"
# Short names accepted by --nodes (besides "both" and the full node names); also used in slugs.
NODE_ALIASES = {"server2": SERVER2, "server1": SERVER1, "thinkpad": THINKPAD}
NODE_SHORT = {SERVER2: "s2", SERVER1: "s1", THINKPAD: "tp"}
RESULTS_ROOT = ROOT / "k8s/results"
DATASETS = {
    # retrieval-extrait with a BERT training set from a run's embedding decision: the B
    # records of those pairs (true pairs and hard negatives) are held out of the stream
    # (exp/build_bert_hard_negatives.py).
    "retrieval-extrait-hn": {
        "data_source_A": "/data/rtas/retrieval-extrait-hn/table_A.csv",
        "data_source_B": "/data/rtas/retrieval-extrait-hn/table_B.csv",
        "ground_truth": "/data/rtas/retrieval-extrait-hn/matches.txt",
        "trainset_path": "/data/rtas/retrieval-extrait-hn/train.csv",
        "evalset_path": "/data/rtas/retrieval-extrait-hn/eval.csv",
        "record_id_source_field": "id",
        "top_k": 3,
        # Records per window (kafka.window_count); large windows spread the per-window
        # checkpoint and snapshot costs over more records.
        "window_count": 6750,
        # bert_training.epochs; training keeps the epoch with the lowest eval loss.
        "bert_epochs": 5,
        # bert_matching.max_matches_per_left/right without the decision: every A matches
        # exactly one B, a B may match several A (A holds several offers of one product).
        "max_matches": (1, None),
        # Default --bert-threshold: best projected F1 of exp/bert_threshold_eval.py on the
        # round-5 candidates (0.95 keeps 95% of true pairs, 59% of false ones).
        "bert_threshold": 0.95,
    },
    # Superseded by retrieval-extrait-hn: its 800 training pairs (half random negatives,
    # 2 epochs) left BERT's probabilities squeezed between 0.3 and 0.75.
    # "retrieval-extrait": {
    #     "data_source_A": "/data/rtas/retrieval-extrait/table_A.csv",
    #     "data_source_B": "/data/rtas/retrieval-extrait/table_B.csv",
    #     "ground_truth": "/data/rtas/retrieval-extrait/matches.txt",
    #     "trainset_path": "/data/rtas/retrieval-extrait/train.csv",
    #     "evalset_path": "/data/rtas/retrieval-extrait/eval.csv",
    #     "record_id_source_field": "id",
    #     "top_k": 3,
    #     "window_count": 6750,
    # },
    "8-movie": {
        "data_source_A": "/data/rtas/8-movie/table_A.csv",
        "data_source_B": "/data/rtas/8-movie/table_B.csv",
        "ground_truth": "/data/rtas/8-movie/matches.txt",
        "trainset_path": "/data/rtas/8-movie/train.csv",
        "evalset_path": "/data/rtas/8-movie/eval.csv",
        "record_id_source_field": "id",
        "top_k": 3,
        "window_count": 450,
        "max_matches": (1, 1),  # one-to-one
    },
    # "large1-pdc": {
    #     "data_source_A": "/data/large_datasets/large1-pdc/tableA.jsonl",
    #     "data_source_B": "/data/large_datasets/large1-pdc/tableB.jsonl",
    #     "ground_truth": "/data/large_datasets/large1-pdc/matches.txt",
    #     "top_k": 5,
    # },
}
# Test settings of one node; NODE_OVERRIDES replaces top-level keys for each node.
# BERT values are "cuda" or "cpu:<cores>" (bert_matching.cpus: Pod CPUs and torch threads).
DEFAULT_NODE_CONFIG: dict[str, Any] = {
    # Offline BERT training device, used once per dataset in the batch phase.
    "bert_training_device": "cuda",
    # capacity design: label -> (random-walk processes, Gensim workers, BERT matching).
    # See capacity_points for how these were sized.
    "capacity": {
        "bert-cpu8-w2v2": (1, 2, "cpu:8"),
        "bert-cpu12-w2v2": (1, 2, "cpu:12"),
        "bert-gpu-w2v1": (1, 1, "cuda"),
        "bert-gpu-w2v2": (1, 2, "cuda"),
        "bert-gpu-w2v4": (1, 4, "cuda"),
        "bert-gpu-w2v8": (1, 8, "cuda"),
    },
    # single design: one configuration per dataset, the fastest one, run once.
    "single": {"bert-gpu-w2v8": (1, 8, "cuda")},
    # sweep design: swept values per stage.
    "sweep": {
        "random-walk": [1, 2, 4, 8],
        "embedding": [1, 2, 4, 8],
        "bert": ["cuda", "cpu:2", "cpu:4", "cpu:8"],
    },
}
NODE_OVERRIDES: dict[str, dict[str, Any]] = {
    SERVER2: {},
    # Virtual machine on a shared host (16 CPUs, 47 GB, Quadro RTX 4000); its data volumes are
    # NFS on server2. It reuses server2's seed (same BERT and initial embedding), so runs on
    # both nodes start from identical models; "seed_node" also skips its offline preparation.
    SERVER1: {"seed_node": SERVER2},
    # Fewer than 8 allocatable CPUs and no GPU.
    THINKPAD: {
        "bert_training_device": "cpu",
        "capacity": {"bert-cpu6-w2v2": (1, 2, "cpu:6")},
        "single": {"bert-cpu4-w2v2": (1, 2, "cpu:4")},
        "sweep": {"random-walk": [1, 2, 4], "embedding": [1, 2, 4], "bert": ["cpu:4"]},
    },
}
# BERT CPU cores when a point does not choose them; equals the manifest's former CPU limit.
DEFAULT_BERT_CPUS = 2
# CPU requested by the incremental stages that are not configured here (similarity,
# candidate enumeration, graph construction, Kafka, ...), on the same node.
OTHER_STAGES_CPUS = 2
# Scheduling task names of each swept stage. Only the incremental phase is swept: the batch
# phase is the offline preparation.
SWEEP_TASKS = {
    "random-walk": {"incremental": ["random-walk"]},
    "embedding": {"incremental": ["embedding-training"]},
    "bert": {"incremental": ["bert-matching"]},
}
ENERGY_PROVIDERS = ("ecofloc", "alumet", "rapl")
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
) + ("alumet_workload_attributed_energy_j", "rapl_incremental_energy_j", "rapl_idle_power_w")
RUN_FIELDS = (
    "dataset", "sweeps", *POINT_FIELDS, "repetition", "base_node", "seed_store", "window_count",
    "top_k", "decision_making", "bert_threshold", "max_matches", "status", "run_dir", "all_mutual_topk_f1", "left_target_only_f1",
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
        node = short_node
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


def short_node(name: str) -> str:
    return NODE_SHORT.get(name, re.sub(r"[^A-Za-z0-9]+", "", name))


def node_config(node: str) -> dict[str, Any]:
    return {**DEFAULT_NODE_CONFIG, **NODE_OVERRIDES.get(node, {})}


def node_point(node: str, processes: int, workers: int, bert: str) -> Point:
    """A point with every configured stage on one node."""
    device, _, cores = str(bert).partition(":")
    return Point(processes, node, workers, node, device, node, int(cores or DEFAULT_BERT_CPUS))


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
    baseline: Point, sweeps: list[str], matrix: dict[str, list[Any]],
) -> dict[Point, list[str]]:
    """Map each distinct point of one node to the sweeps it belongs to."""
    points: dict[Point, list[str]] = {}
    for sweep in sweeps:
        for value in matrix.get(sweep, []):
            if sweep == "random-walk":
                point = replace(baseline, random_walk_processes=value)
            elif sweep == "embedding":
                point = replace(baseline, gensim_workers=value)
            else:
                device, _, cores = str(value).partition(":")
                point = replace(baseline, bert_device=device,
                                bert_cpus=int(cores or baseline.bert_cpus))
            points.setdefault(point, []).append(sweep)
    return points


def capacity_points(node: str, design: dict[str, tuple[int, int, str]]) -> dict[Point, list[str]]:
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
    points: dict[Point, list[str]] = {}
    for label, (processes, workers, bert) in design.items():
        points.setdefault(node_point(node, processes, workers, bert), []).append(label)
    return points


def run(
    command: list[str], *, check: bool = True, capture: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(command), flush=True)
    return subprocess.run(
        command, cwd=ROOT, check=check, text=True, capture_output=capture,
        env={**os.environ, **env} if env else None,
    )


def alumet_env(nodes: list[str]) -> dict[str, str]:
    """Limit Alumet collection, checks and totals to the nodes a run uses."""
    return {"ERCTL_ALUMET_NODES": ",".join(nodes)}


ECOFLOC_NODES_FILE = ROOT / "k8s/monitoring/ecofloc/energy-nodes.conf"


def write_ecofloc_nodes_file(output: Path, node: str) -> Path:
    """An EcoFLOC node list with only ``node``: its preflight then checks, and its energy
    covers, the node a run uses instead of every node in energy-nodes.conf."""
    entries = []
    for line in ECOFLOC_NODES_FILE.read_text(encoding="utf-8").splitlines():
        spec = line.split("#", 1)[0].strip()
        if spec and spec.split("=", 1)[0].strip() == node:
            entries.append(spec)
    if not entries:
        raise RuntimeError(f"{ECOFLOC_NODES_FILE} has no EcoFLOC entry for {node}")
    path = output / "energy" / f"ecofloc-nodes-{short_node(node)}.conf"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(entries) + "\n", encoding="utf-8")
    return path


class RaplSampler:
    """RAPL+NVML sampling over the whole benchmark (k8s/monitoring/rapl/rapl.py): an idle
    baseline first, then each run's energy is cut from the samples by its time interval."""

    def __init__(self, output: Path, nodes: list[str], idle_seconds: int):
        spec = importlib.util.spec_from_file_location("rapl", ROOT / "k8s/monitoring/rapl/rapl.py")
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.nodes, self.idle_seconds = nodes, idle_seconds
        self.samples = output / "rapl" / datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        self.baselines: list[tuple[float, float]] = []

    def start(self) -> None:
        if not self.module.preflight(self.module.load_nodes(self.nodes)):
            raise RuntimeError("RAPL preflight failed (see above)")
        self.module.start(self.samples, self.nodes)
        atexit.register(self.module.stop_sampler, self.samples)
        self.baselines.append(self.idle("idle-before"))

    def idle(self, label: str) -> tuple[float, float]:
        print(f"[rapl] idle baseline {label}: {self.idle_seconds} s", flush=True)
        self.module.mark(self.samples, f"{label}-start")
        start = time.time()
        time.sleep(self.idle_seconds)
        self.module.mark(self.samples, f"{label}-end")
        return start, time.time()

    def record(self, run_dir: Path | None, started: str, finished: str) -> None:
        if not run_dir:
            return
        # The samplers write every second: wait until samples past the end of the run exist,
        # otherwise its last second has no sample after it and the interval looks uncovered.
        time.sleep(max(0.0, self.module.parse_time(finished) + 5 - time.time()))
        try:
            summary = self.module.summarize(
                self.samples, self.module.parse_time(started), self.module.parse_time(finished), self.baselines,
            )
        except (OSError, ValueError, KeyError) as error:
            print(f"[rapl] no summary for {run_dir.name}: {error}", file=sys.stderr)
            return
        path = run_dir / "energy" / "rapl-summary.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    def finish(self, output: Path) -> None:
        """A second idle period, summarized against the first one: its incremental energy
        should be close to zero; otherwise the idle power drifted during the benchmark."""
        after = self.idle("idle-after")
        time.sleep(5)  # samples past the end of the idle period, as in record()
        drift = self.module.summarize(self.samples, *after, self.baselines)
        (output / "rapl-idle-drift.json").write_text(json.dumps(drift, indent=2), encoding="utf-8")
        print(f"[rapl] idle drift over {self.idle_seconds} s: {drift['incremental_by_component_j']} J "
              f"(baseline {[n['idle_power_w'] for n in drift['nodes'].values()]} W)")
        self.module.stop_sampler(self.samples)


def node_env(node: str, ecofloc_nodes_file: Path | None) -> dict[str, str]:
    """Environment of a pipeline run on ``node``: energy monitors and the seed Job stay on it."""
    env = {**alumet_env([node]), "SEED_JOB_NODE": node}
    if ecofloc_nodes_file is not None:
        env["ENERGY_NODES_FILE"] = str(ecofloc_nodes_file)
    return env


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


def seed_on_disk(seed_store: str) -> Path | None:
    """The seed directory when this host is the NFS server and the seed is complete: the seed
    Job publishes graph/embedding/index/bert in one rename (k8s/pipeline/pipeline.sh)."""
    manifest = ROOT / "k8s/datasets/seed-volume.yaml"
    if not manifest.exists():
        return None
    match = re.search(r"^\s*path:\s*(\S+)", manifest.read_text(encoding="utf-8"), re.MULTILINE)
    seed = Path(match.group(1)) / seed_store if match else None
    try:
        complete = seed is not None and all((seed / part).is_dir() for part in ("graph", "embedding", "index", "bert"))
    except PermissionError:
        return None
    return seed if complete else None


def seed_store_name(dataset: str, node: str) -> str:
    return f"serie1-{dataset}-{short_node(node_config(node).get('seed_node', node))}"


def write_config(
    base: dict[str, Any], destination: Path, dataset: str, point: Point,
    bert_training_device: str, record_interval_ms: int | None = None, decision_making: bool = True,
    bert_threshold: float | None = None,
) -> None:
    config = dict(base)
    dataset_config = DATASETS[dataset]
    internal_keys = {
        "top_k", "record_id_source_field", "window_count", "bert_epochs", "max_matches", "bert_threshold",
    }
    config.update({key: value for key, value in dataset_config.items() if key not in internal_keys})
    # Model, index and record paths include version_name, so it must match the seed's.
    config["version_name"] = f"bench-{dataset}"
    # A fresh topic and consumer group per run: the shared default topic keeps records an
    # earlier run produced but never consumed, and the next consumer would read them first.
    run_token = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    config["kafka"] = dict(
        base.get("kafka", {}), topicid=f"eaer-serie1-{run_token}", groupid=f"eaer-serie1-{run_token}",
    )
    if "window_count" in dataset_config:
        config["kafka"]["window_count"] = int(dataset_config["window_count"])
    if record_interval_ms is not None:
        config["kafka"]["record_interval_ms"] = int(record_interval_ms)
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
    if bert_threshold is None:
        bert_threshold = dataset_config.get("bert_threshold")
    if bert_threshold is not None:
        config["bert_matching"]["threshold"] = float(bert_threshold)
    if "max_matches" in dataset_config:
        left, right = dataset_config["max_matches"]
        config["bert_matching"]["max_matches_per_left"] = left
        config["bert_matching"]["max_matches_per_right"] = right
    if point.bert_device == "cpu":
        config["bert_matching"]["cpus"] = point.bert_cpus
    else:
        config["bert_matching"].pop("cpus", None)
    # BERT training only runs in the offline preparation, once per dataset.
    config["bert_training"] = dict(
        base.get("bert_training", {}) or {}, device=bert_training_device, seed=1729,
    )
    if "bert_epochs" in dataset_config:
        config["bert_training"]["epochs"] = int(dataset_config["bert_epochs"])
    # One top_k for both the window's mutual top-k and the decision. Decision making off:
    # BERT matching judges each window's candidates directly (bert_matching is always on).
    top_k = int(dataset_config["top_k"])
    config["calculating_similarity"] = dict(base.get("calculating_similarity", {}), top_k=top_k)
    config["decision_making"] = dict(
        base.get("decision_making", {}), top_k=top_k, enabled=decision_making,
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
    for provider, components_key in (
        ("ecofloc", "by_metric_j"), ("alumet", "hardware_by_device_j"), ("rapl", "by_component_j"),
    ):
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
        if provider == "rapl":
            row["rapl_incremental_energy_j"] = data.get("incremental_energy_j", "")
            row["rapl_idle_power_w"] = round(sum(
                power for node in data.get("nodes", {}).values()
                for power in node.get("idle_power_w", {}).values()
            ), 3)
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
    env: dict[str, str],
) -> tuple[subprocess.CompletedProcess[str], Path | None, str, str]:
    before = {path.resolve() for path in RESULTS_ROOT.glob("embedding-training-inference-evaluation-*")}
    started = datetime.now(timezone.utc).isoformat()
    result = run([
        "bash", "k8s/erctl.sh", "pipeline", "start", "-c", str(config_path),
        "--scheduling-config", str(scheduling_config),
        "--phase", phase, "--seed-store", seed_store,
        *(["--energy-monitor", energy_monitor] if energy_monitor != "none" else []), "--results-summary",
    ], check=False, env=env)
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
        "--design", choices=("single", "capacity", "sweep"), default="single",
        help="single: one fastest configuration per dataset; capacity: whole-pipeline "
             "configurations (capacity_points); sweep: one factor at a time",
    )
    parser.add_argument(
        "--decision-making", choices=("on", "off"), default="on",
        help="on: BERT filters the embedding decision; off: BERT judges each window's mutual "
             "top-k candidates directly (the decision is skipped)",
    )
    parser.add_argument(
        "--bert-threshold", type=float, default=None,
        help="bert_matching.threshold for every dataset (default: the dataset's bert_threshold, else the "
             "base config's 0.5); see exp/bert_threshold_eval.py",
    )
    parser.add_argument(
        "--record-interval-ms", type=int, default=None,
        help="Producer pause between records (default: the simulator image's csv.file.timeout, "
             "50 ms); lower values shorten the stream and change the stream rate",
    )
    parser.add_argument("--datasets", default=",".join(DATASETS), help="Comma-separated dataset names")
    parser.add_argument("--sweeps", default=",".join(SWEEP_TASKS),
                        help="sweep design only: comma-separated subset of: " + ",".join(SWEEP_TASKS))
    parser.add_argument(
        "--nodes", default="server2",
        help="server2 (default), server1, thinkpad, or both (= server2 + thinkpad) (also comma-separated full node names); every "
             "stage of a run executes on one node and each selected node runs every test",
    )
    parser.add_argument("--baseline-processes", type=int, default=1, help="Random-walk processes when not swept")
    parser.add_argument("--baseline-workers", type=int, default=1, help="Gensim workers when not swept")
    parser.add_argument(
        "--baseline-bert-device", choices=("cpu", "cuda"), default="cpu",
        help="BERT matching device when not swept",
    )
    parser.add_argument(
        "--bert-training-device", choices=("cpu", "cuda"), default=None,
        help="Offline BERT training device on every node (default: the node's bert_training_device)",
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
    parser.add_argument(
        "--energy-monitor", choices=("ecofloc", "alumet", "ecofloc-alumet", "none"), default="ecofloc-alumet",
        help="none: neither (e.g. with --rapl only, or on server1 where both fail inside the VM)",
    )
    parser.add_argument(
        "--rapl", action="store_true",
        help="Also measure RAPL (CPU, DRAM) and NVML (GPU) energy with an idle baseline "
             "(k8s/monitoring/rapl); virtual nodes are measured on their physical host",
    )
    parser.add_argument("--idle-seconds", type=int, default=600,
                        help="Idle baseline before and after the benchmark with --rapl (default 600)")
    parser.add_argument("--output", type=Path, default=None,
                        help="default: reports/serie1-capacity or reports/serie1-motivation")
    parser.add_argument("--dry-run", action="store_true", help="Generate configs without starting workloads")
    parser.add_argument("--prep-only", action="store_true",
                        help="Stop after the offline preparation (e.g. to evaluate the trained BERT first)")
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
    if not sweeps or set(sweeps) - set(SWEEP_TASKS):
        parser.error(f"--sweeps accepts only: {', '.join(SWEEP_TASKS)}")
    nodes: list[str] = []
    for value in split_list(args.nodes):
        selected = [SERVER2, THINKPAD] if value == "both" else [NODE_ALIASES.get(value, value)]
        for node in selected:
            if node not in NODE_OVERRIDES:
                parser.error(f"unknown node {value}; use server2, server1, thinkpad, both, or a NODE_OVERRIDES key")
            if node not in nodes:
                nodes.append(node)
    if not nodes:
        parser.error("--nodes selects no node")
    if args.repetitions < 1 or args.baseline_processes < 1 or args.baseline_workers < 1:
        parser.error("--repetitions, --baseline-processes and --baseline-workers must be positive")

    # Per node: its settings, the sweep baseline, and the offline-preparation point (the
    # highest sweep setting of every stage).
    configs = {node: node_config(node) for node in nodes}
    training_device = {
        node: args.bert_training_device or configs[node]["bert_training_device"] for node in nodes
    }
    baselines: dict[str, Point] = {}
    prep_points: dict[str, Point] = {}
    points: dict[Point, list[str]] = {}
    for node in nodes:
        matrix = configs[node]["sweep"]
        baselines[node] = node_point(
            node, args.baseline_processes, args.baseline_workers, args.baseline_bert_device,
        )
        bert_values = [str(value).partition(":")[0] for value in matrix.get("bert", [])]
        prep_points[node] = node_point(
            node,
            max(matrix.get("random-walk") or [args.baseline_processes]),
            max(matrix.get("embedding") or [args.baseline_workers]),
            "cuda" if "cuda" in bert_values else args.baseline_bert_device,
        )
        node_points = capacity_points(node, configs[node][args.design]) \
            if args.design in ("single", "capacity") \
            else build_points(baselines[node], sweeps, matrix)
        points.update(node_points)
        if not args.dry_run:
            validate_cluster(
                [*node_points, baselines[node], prep_points[node]], node, training_device[node],
            )
    if not points:
        parser.error("the selected sweeps and nodes produce an empty matrix")
    base = yaml.safe_load(args.base_config.read_text(encoding="utf-8"))
    if not isinstance(base, dict):
        raise RuntimeError("base config root must be a YAML object")

    decision_making = args.decision_making == "on"
    default_output = {
        "single": "reports/serie1-single", "capacity": "reports/serie1-capacity",
    }.get(args.design, "reports/serie1-motivation")
    # Matching settings change the results, so each gets its own directory and resume state.
    top_ks = "-".join(sorted({str(DATASETS[dataset]["top_k"]) for dataset in datasets}))
    default_output += f"-k{top_ks}-{'decision' if decision_making else 'bert-only'}"
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
    ecofloc_files = {
        node: write_ecofloc_nodes_file(output, node) for node in nodes
    } if "ecofloc" in args.energy_monitor else {}

    def run_env(node: str) -> dict[str, str]:
        return node_env(node, ecofloc_files.get(node))
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
        # Every stage of a point runs on one node; the BERT node names it.
        node = point.bert_node
        config_path = configs_dir / f"{dataset}-{point.slug()}.yaml"
        write_config(base, config_path, dataset, point, training_device[node], args.record_interval_ms,
                     decision_making=decision_making, bert_threshold=args.bert_threshold)
        return config_path, write_scheduling_config(output, node, point)

    def run_offline_prep(dataset: str, node: str) -> int:
        """Train once per dataset and node at the highest configuration and save the seed."""
        seed_store = seed_store_name(dataset, node)
        seed_node = node_config(node).get("seed_node", node)
        if seed_node != node:
            print(f"[prep] {dataset} on {node} reuses {seed_node}'s seed {seed_store}")
            return 0
        prep_point = prep_points[node]
        def built(rows: list[dict[str, str]]) -> bool:
            return any(
                row.get("dataset") == dataset and row.get("seed_store") == seed_store
                and row.get("status") == "Succeeded" for row in rows
            )
        # A seed is shared by every benchmark on this node: one built for another output
        # directory is reused, so all of them start from the same models.
        elsewhere = [path for path in sorted((ROOT / "reports").glob("*/offline-prep.csv"))
                     if path != prep_path and built(load_rows(path))]
        on_disk = seed_on_disk(seed_store)
        if (built(prep_rows) or elsewhere or on_disk) and not args.rebuild_seeds:
            where = ("" if built(prep_rows) else f" (recorded in {elsewhere[-1].relative_to(ROOT)})" if elsewhere
                     else f" (found in {on_disk})")
            print(f"[prep] {dataset} seed {seed_store} already built{where}")
            return 0
        config_path = configs_dir / f"{dataset}-{short_node(node)}-offline-prep.yaml"
        write_config(base, config_path, dataset, prep_point, training_device[node],
                     decision_making=decision_making)
        scheduling_config = write_scheduling_config(output, node, prep_point)
        print(f"[prep] {dataset} seed {seed_store} with {prep_point.slug()}", flush=True)
        if args.dry_run:
            return 0
        result, run_dir, started, finished = start_pipeline(
            config_path, scheduling_config, args.energy_monitor, "batch", seed_store, run_env(node),
        )
        if rapl:
            rapl.record(run_dir, started, finished)
        manifest = read_json(run_dir / "manifest.json") if run_dir else {}
        prep_rows[:] = [
            row for row in prep_rows
            if (row.get("dataset"), row.get("seed_store")) != (dataset, seed_store)
        ]
        prep_rows.append({
            "dataset": dataset, "seed_store": seed_store, **prep_point.as_row(),
            "bert_training_device": training_device[node], "base_node": node,
            "status": str(manifest.get("status") or ("Succeeded" if result.returncode == 0 else "Failed")),
            "run_dir": str(run_dir.relative_to(ROOT)) if run_dir else "",
            **(extract_energy(run_dir) if run_dir else {}),
            "started_at": started, "finished_at": finished, "return_code": result.returncode,
        })
        write_csv(prep_path, PREP_FIELDS, prep_rows)
        return result.returncode

    if "alumet" in args.energy_monitor and not args.dry_run:
        # Collect only on the selected nodes; collectors elsewhere are removed.
        run(["bash", "k8s/erctl.sh", "alumet", "start"], env=alumet_env(nodes))
    # server2 serves the volumes (NFS) and Kafka to every node: a run on server1 also costs
    # energy there, so server2 is always sampled (its idle baseline cancels when unused).
    rapl = RaplSampler(output, sorted({*nodes, SERVER2}), args.idle_seconds) \
        if args.rapl and not args.dry_run else None
    if rapl:
        rapl.start()

    for dataset in datasets:
        for node in nodes:
            code = run_offline_prep(dataset, node)
            if code != 0:
                print(f"Offline preparation failed for {dataset} on {node}; see {prep_path}",
                      file=sys.stderr)
                return code

    if args.prep_only:
        print(f"Offline preparation done (--prep-only): {prep_path}")
        return 0

    if args.warmup and not args.dry_run:
        for node in nodes:
            print(f"Warm-up run on {node} (not recorded)", flush=True)
            config_path, scheduling_config = prepare(datasets[0], baselines[node])
            start_pipeline(
                config_path, scheduling_config, args.energy_monitor, "incremental",
                seed_store_name(datasets[0], node), run_env(node),
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

        seed_store = seed_store_name(dataset, point.bert_node)
        result, run_dir, started, finished = start_pipeline(
            config_path, scheduling_config, args.energy_monitor, "incremental", seed_store,
            run_env(point.bert_node),
        )
        if rapl:
            rapl.record(run_dir, started, finished)
        manifest = read_json(run_dir / "manifest.json") if run_dir else {}
        status = str(manifest.get("status") or ("Succeeded" if result.returncode == 0 else "Failed"))
        all_f1, left_f1 = extract_f1(run_dir) if run_dir else ("", "")
        row: dict[str, Any] = {
            "dataset": dataset, "sweeps": "+".join(points[point]), **point.as_row(),
            "repetition": repetition, "base_node": point.bert_node, "seed_store": seed_store,
            "window_count": DATASETS[dataset].get("window_count", base.get("kafka", {}).get("window_count", "")),
            "top_k": DATASETS[dataset]["top_k"], "decision_making": args.decision_making,
            "bert_threshold": next((value for value in (args.bert_threshold, DATASETS[dataset].get("bert_threshold"))
                                    if value is not None), ""),
            "max_matches": "/".join(str(v or "any") for v in DATASETS[dataset].get("max_matches", (None, None))),
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
    if rapl:
        rapl.finish(output)
    print(f"Offline preparation (recorded, not analyzed): {prep_path}")
    print(f"Run summary: {runs_path}")
    print(f"Per-stage timings: {stages_path}")
    print(f"Incremental per-stage hardware energy: {energy_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
