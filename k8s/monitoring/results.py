#!/usr/bin/env python3
"""Persist and display matching, placement, evaluation, and monitoring results."""
from __future__ import annotations

import argparse
import bisect
import csv
import importlib.util
import json
import math
import os
import re
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path


INCREMENTAL_JOBS = {
    "bert-matching", "calculating-similarity", "candidate-enumeration", "cg-feature-extraction",
    "embedding-training", "evaluation", "graph-construction",
    "kafka-consumer", "kafka-producer", "normalization", "random-walk",
}
PLACEMENT_FIELDS = (
    "phase", "task", "pod", "pod_uid", "node", "pod_ip", "status", "started_at",
    "finished_at", "workflow", "job",
)
STEP_METRIC_FIELDS = (
    "phase", "task", "pod", "pod_uid", "node", "status", "attempt",
    "logical_read_bytes", "logical_write_bytes", "storage_read_bytes",
    "storage_write_bytes", "elapsed_seconds", "pod_elapsed_seconds",
    "wait_seconds", "compute_seconds", "windows",
    "transfer_seconds", "transfer_read_bytes", "transfer_write_bytes", "transfers",
    "started_at", "finished_at",
)
# One row per processed window (plus one "setup" and one "eos" row) of every stage Pod.
# wait_seconds = blocked on a peer; compute_seconds = everything else in that window;
# transfer_seconds = the part of compute_seconds spent moving inter-stage data (NFS I/O).
WINDOW_METRIC_FIELDS = (
    "phase", "task", "pod", "node", "window", "wait_seconds", "compute_seconds",
    "transfer_seconds", "transfer_read_bytes", "transfer_write_bytes",
    "started_at", "ended_at",
)
# One row per inter-stage read or write; its wall-clock bounds key the energy lookup.
TRANSFER_METRIC_FIELDS = (
    "phase", "task", "pod", "node", "direction", "kind", "bytes", "seconds",
    "started_at", "ended_at",
)
STEP_METRICS_PREFIX = "[EAER_STEP_METRICS] "
WINDOW_METRICS_PREFIX = "[EAER_WINDOW_METRICS] "
TRANSFER_METRICS_PREFIX = "[EAER_TRANSFER_METRICS] "
ECOFLOC_METRICS = ("cpu", "gpu", "nic", "ram", "sd")
POD_UID_PATTERN = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
ECOFLOC_SESSION_FIELDS = (
    "node", "pid", "process_start", "pod_uid", "container_id", "metric",
    "average_power_w", "total_energy_j", "status", "started_at", "ended_at", "task",
)


def parse_ecofloc_log(path: Path) -> tuple[float, float] | None:
    text = path.read_text(encoding="utf-8", errors="replace")
    number = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)"
    average = re.findall(rf"Average\s+Power\s*:\s*({number})", text, re.IGNORECASE)
    total = re.findall(rf"Total.*?Energy\s*:\s*({number})", text, re.IGNORECASE)
    if not average or not total:
        return None
    values = float(average[-1]), float(total[-1])
    return values if all(math.isfinite(value) for value in values) else None


def ecofloc_sessions(energy_dir: Path) -> list[dict[str, str]]:
    sessions_path = energy_dir / "sessions.tsv"
    if sessions_path.exists():
        with sessions_path.open(encoding="utf-8") as stream:
            sessions = list(csv.DictReader(stream, delimiter="\t"))
    else:
        sessions = []

    processes_path = energy_dir / "processes.tsv"
    processes: dict[tuple[str, str], dict[str, str]] = {}
    if processes_path.exists():
        with processes_path.open(encoding="utf-8") as stream:
            for row in csv.DictReader(stream, delimiter="\t"):
                processes[(row.get("node", ""), row.get("pid", ""))] = row

    indexed = {
        (row.get("node", ""), row.get("pid", ""), row.get("metric", "").lower()): row
        for row in sessions
    }
    changed = False
    pattern = re.compile(r"^.+_(\d+)_(cpu|gpu|nic|ram|sd)\.log$", re.IGNORECASE)
    for path in sorted((energy_dir / "logs").glob("*/*.log")):
        match = pattern.match(path.name)
        if not match:
            continue
        node = path.parent.name
        pid, metric = match.group(1), match.group(2).lower()
        measured = parse_ecofloc_log(path)
        if measured is None:
            continue
        average, total = measured
        key = node, pid, metric
        row = indexed.get(key)
        if row is None:
            process = processes.get((node, pid))
            if process is None:
                continue
            sibling = next(
                (item for item in sessions if item.get("node") == node and item.get("pid") == pid),
                {},
            )
            row = {
                "node": node,
                "pid": pid,
                "process_start": process.get("process_start", ""),
                "pod_uid": process.get("pod_uid", ""),
                "container_id": process.get("container_id", ""),
                "metric": metric,
                "average_power_w": "",
                "total_energy_j": "",
                "status": "",
                "started_at": sibling.get("started_at", ""),
                "ended_at": sibling.get("ended_at", ""),
                "task": process.get("task", "unknown"),
            }
            sessions.append(row)
            indexed[key] = row
        parsed_average = f"{average:g}"
        parsed_total = f"{total:g}"
        if (
            row.get("average_power_w") != parsed_average
            or row.get("total_energy_j") != parsed_total
            or row.get("status") != "ok"
        ):
            row["average_power_w"] = parsed_average
            row["total_energy_j"] = parsed_total
            row["status"] = "ok"
            changed = True

    if changed:
        with sessions_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=ECOFLOC_SESSION_FIELDS, delimiter="\t")
            writer.writeheader()
            writer.writerows({field: row.get(field, "") for field in ECOFLOC_SESSION_FIELDS} for row in sessions)
    return sessions


def task_from_pod_name(pod: str, workflow: str) -> str:
    name = pod.removeprefix(f"{workflow}-") if workflow else pod
    parts = name.rsplit("-", 1)
    return parts[0] if len(parts) == 2 and parts[1].isdigit() else name


def workflow_tasks(namespace: str, workflow: str) -> tuple[dict[str, str], dict[str, str]]:
    result = subprocess.run(
        ["kubectl", "get", "workflow", "-n", namespace, workflow, "-o", "json"],
        text=True, capture_output=True, check=True,
    )
    nodes = json.loads(result.stdout).get("status", {}).get("nodes", {}) or {}
    by_pod: dict[str, str] = {}
    by_node_name: dict[str, str] = {}
    for node_id, node in nodes.items():
        if node.get("type") != "Pod":
            continue
        task = node.get("displayName") or node.get("templateName") or ""
        if not task:
            continue
        for pod_name in (node.get("podName"), node.get("id"), node_id):
            if pod_name:
                by_pod[pod_name] = task
        if node.get("name"):
            by_node_name[node["name"]] = task
    return by_pod, by_node_name


def energy_summary(run_dir: Path) -> None:
    energy_dir = run_dir / "energy"
    energy_dir.mkdir(parents=True, exist_ok=True)

    by_task: dict[str, float] = defaultdict(float)
    by_node: dict[str, float] = defaultdict(float)
    by_metric: dict[str, float] = defaultdict(float)
    by_task_metric: dict[str, float] = defaultdict(float)
    by_pod: dict[str, float] = defaultdict(float)
    by_pod_metric: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    pod_by_uid: dict[str, str] = {}
    placement_path = run_dir / "placement.tsv"
    if placement_path.exists():
        with placement_path.open(encoding="utf-8") as stream:
            pod_by_uid = {
                row["pod_uid"]: row["pod"]
                for row in csv.DictReader(stream, delimiter="\t")
                if row.get("pod_uid") and row.get("pod")
            }
    sessions = []
    valid_sessions = 0
    invalid_sessions = 0
    for row in ecofloc_sessions(energy_dir):
        node = row.get("node", "")
        metric = row.get("metric", "")
        try:
            energy = float(row.get("total_energy_j", ""))
            numeric = math.isfinite(energy)
        except (TypeError, ValueError):
            energy = 0.0
            numeric = False
        task = row.get("task") or "unknown"
        pod_uid = row.get("pod_uid", "")
        pod = pod_by_uid.get(pod_uid) or pod_uid or "unknown"
        metric = metric.lower()
        row["task"] = task
        row["pod"] = pod
        sessions.append(row)
        by_pod.setdefault(pod, 0.0)
        by_pod_metric.setdefault(pod, defaultdict(float))
        if numeric and row.get("status") == "ok":
            valid_sessions += 1
            by_task[task] += energy
            by_node[node] += energy
            by_metric[metric] += energy
            by_task_metric[f"{task}|{metric}"] += energy
            by_pod[pod] += energy
            by_pod_metric[pod][metric] += energy
        else:
            invalid_sessions += 1

    agent_status: dict[str, str] = {}
    agents_path = energy_dir / "agents.tsv"
    if agents_path.exists():
        with agents_path.open(encoding="utf-8") as stream:
            for row in csv.DictReader(stream, delimiter="\t"):
                if row.get("node"):
                    agent_status[row["node"]] = row.get("status", "unknown")

    unhealthy_agents = {
        node: status for node, status in agent_status.items()
        if status not in {"ready", "completed"}
    }
    if valid_sessions == 0:
        measurement_status = "failed"
    elif invalid_sessions or unhealthy_agents:
        measurement_status = "partial"
    else:
        measurement_status = "complete"

    summary = {
        "provider": "ecofloc",
        "total_energy_j": round(sum(by_node.values()), 6),
        "measurement_status": measurement_status,
        "valid_session_count": valid_sessions,
        "invalid_session_count": invalid_sessions,
        "agents": agent_status,
        "by_task_j": {key: round(value, 6) for key, value in sorted(by_task.items())},
        "by_pod_j": {key: round(value, 6) for key, value in sorted(by_pod.items())},
        "by_pod_metric_j": {
            pod: {metric: round(value, 6) for metric, value in sorted(metrics.items())}
            for pod, metrics in sorted(by_pod_metric.items())
        },
        "by_node_j": {key: round(value, 6) for key, value in sorted(by_node.items())},
        "by_metric_j": {key: round(value, 6) for key, value in sorted(by_metric.items())},
        "by_task_metric_j": {key: round(value, 6) for key, value in sorted(by_task_metric.items())},
        "note": (
            "total_energy_j is the sum over CPU/RAM/SD/NIC/GPU per process; it is an aggregate "
            "across components, correct only if EcoFLOC reports these as non-overlapping figures. "
            "See by_pod_metric_j for the per-Pod breakdown; absent metrics were not measured "
            "successfully and are displayed as n/a."
        ),
        "sessions": sessions,
    }
    (energy_dir / "ecofloc-summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    if measurement_status == "failed":
        raise RuntimeError("EcoFLOC produced no valid measurement sessions")
    if measurement_status == "partial":
        print(
            "Warning: EcoFLOC measurement is partial; inspect ecofloc-summary.json and raw logs.",
            file=sys.stderr,
        )


def detect_artifacts(run_dir: Path) -> dict[str, bool]:
    # Report what was actually persisted, so a Succeeded run whose matching artifacts failed
    # to copy is not silently reported as fully saved.
    matching = run_dir / "matching"
    graph = report = None
    if (matching / "predicted").exists():
        graph = next((matching / "predicted").rglob("predicted_matching.csv"), None)
    if (matching / "communication").exists():
        report = next((matching / "communication").rglob("evaluation_report.json"), None)
    return {
        "energy_summary": any(energy_summary_paths(run_dir)),
        "matching_graph": graph is not None,
        "evaluation_report": report is not None,
        "scheduling_plan": (run_dir / "scheduling-plan.tsv").exists(),
        "data_locality_plan": (run_dir / "data-locality-plan.tsv").exists(),
        "pod_placement": (run_dir / "placement.tsv").exists(),
        "step_metrics": (run_dir / "step-metrics.tsv").exists(),
    }


def collect_placement(run_dir: Path, namespace: str, phase: str, workflow: str) -> None:
    result = subprocess.run(
        ["kubectl", "get", "pods", "-n", namespace, "-o", "json"],
        text=True, capture_output=True, check=True,
    )
    items = json.loads(result.stdout).get("items", [])
    tasks_by_pod: dict[str, str] = {}
    tasks_by_node_name: dict[str, str] = {}
    if phase == "batch" and workflow:
        tasks_by_pod, tasks_by_node_name = workflow_tasks(namespace, workflow)
    rows = []
    for pod in items:
        metadata = pod.get("metadata", {})
        labels = metadata.get("labels", {}) or {}
        annotations = metadata.get("annotations", {}) or {}
        pod_workflow = labels.get("workflows.argoproj.io/workflow", "")
        job = labels.get("job-name", "")
        if phase == "batch":
            if not workflow or pod_workflow != workflow:
                continue
        elif job not in INCREMENTAL_JOBS:
            continue

        status = pod.get("status", {})
        finished = max(
            (state.get("terminated", {}).get("finishedAt", "")
             for container in status.get("containerStatuses", [])
             for state in [container.get("state", {})]),
            default="",
        )
        pod_name = metadata.get("name", "")
        node_name = annotations.get("workflows.argoproj.io/node-name", "")
        task = (
            tasks_by_pod.get(pod_name)
            or tasks_by_node_name.get(node_name)
            or labels.get("workflows.argoproj.io/template")
            or labels.get("app")
            or job
        )
        if phase == "batch" and not task:
            task = task_from_pod_name(pod_name, workflow)
        rows.append({
            "phase": phase,
            "task": task,
            "pod": pod_name,
            "pod_uid": metadata.get("uid", ""),
            "node": pod.get("spec", {}).get("nodeName", ""),
            "pod_ip": status.get("podIP", ""),
            "status": status.get("phase", ""),
            "started_at": status.get("startTime", ""),
            "finished_at": finished,
            "workflow": pod_workflow,
            "job": job,
        })

    path = run_dir / "placement.tsv"
    existing = set()
    if path.exists():
        with path.open(encoding="utf-8") as stream:
            existing = {(row["phase"], row["pod"]) for row in csv.DictReader(stream, delimiter="\t")}
    new_rows = [row for row in rows if (row["phase"], row["pod"]) not in existing]
    if not new_rows:
        return
    run_dir.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=PLACEMENT_FIELDS, delimiter="\t")
        if path.stat().st_size == 0:
            writer.writeheader()
        writer.writerows(new_rows)


def collect_matching(run_dir: Path, namespace: str, local_root: str = "", node: str = "") -> None:
    pod = f"eaer-results-{uuid.uuid4().hex[:12]}"
    manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod, "namespace": namespace},
        "spec": {
            "restartPolicy": "Never",
            "containers": [{
                "name": "reader",
                "image": "busybox:1.36",
                "command": ["sleep", "300"],
                "volumeMounts": [
                    {"name": "predicted", "mountPath": "/data/predicted"},
                    {"name": "communication", "mountPath": "/data/communication"},
                ],
            }],
            "volumes": [],
        },
    }
    if local_root:
        manifest["spec"]["affinity"] = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [{"matchExpressions": [{
                        "key": "kubernetes.io/hostname", "operator": "In", "values": [node]
                    }]}]
                }
            }
        }
        manifest["spec"]["volumes"] = [
            {"name": "predicted", "hostPath": {"path": f"{local_root}/predicted", "type": "Directory"}},
            {"name": "communication", "hostPath": {"path": f"{local_root}/communication", "type": "Directory"}},
        ]
    else:
        manifest["spec"]["volumes"] = [
            {"name": "predicted", "persistentVolumeClaim": {"claimName": "pipeline-decision-evaluation-cache-claim"}},
            {"name": "communication", "persistentVolumeClaim": {"claimName": "pipeline-communication-claim"}},
        ]
    subprocess.run(["kubectl", "apply", "-f", "-"], input=json.dumps(manifest), text=True, check=True)
    try:
        subprocess.run([
            "kubectl", "wait", "-n", namespace, f"pod/{pod}",
            "--for=condition=Ready", "--timeout=2m",
        ], check=True)
        target = run_dir / "matching"
        target.mkdir(parents=True, exist_ok=True)
        subprocess.run(["kubectl", "cp", f"{namespace}/{pod}:/data/predicted", str(target / "predicted")], check=True)
        subprocess.run(["kubectl", "cp", f"{namespace}/{pod}:/data/communication", str(target / "communication")], check=True)
    finally:
        subprocess.run(["kubectl", "delete", "pod", pod, "-n", namespace, "--ignore-not-found"], check=False)


def _elapsed_between(started_at: str, finished_at: str) -> str:
    if not started_at or not finished_at:
        return ""
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        finish = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
        return f"{max(0.0, (finish - start).total_seconds()):.6f}"
    except ValueError:
        return ""


def _pod_log_metrics(
    namespace: str, pod: str,
) -> tuple[dict[str, object] | None, list[dict[str, object]], list[dict[str, object]]]:
    """Return (final step-metrics record, per-window records, per-transfer records) of one Pod."""
    result = subprocess.run(
        ["kubectl", "logs", "-n", namespace, pod, "--tail=-1"],
        text=True, capture_output=True, check=False,
    )
    if result.returncode != 0:
        return None, [], []
    step: dict[str, object] | None = None
    windows: list[dict[str, object]] = []
    transfers: list[dict[str, object]] = []
    for line in result.stdout.splitlines():
        for prefix in (STEP_METRICS_PREFIX, WINDOW_METRICS_PREFIX, TRANSFER_METRICS_PREFIX):
            marker = line.find(prefix)
            if marker < 0:
                continue
            try:
                value = json.loads(line[marker + len(prefix):])
            except json.JSONDecodeError:
                break
            if not isinstance(value, dict):
                break
            if prefix == STEP_METRICS_PREFIX:
                step = value  # the last record wins
            elif prefix == WINDOW_METRICS_PREFIX:
                windows.append(value)
            else:
                transfers.append(value)
            break
    return step, windows, transfers


def collect_step_metrics(run_dir: Path, namespace: str) -> None:
    placement_path = run_dir / "placement.tsv"
    if not placement_path.exists():
        raise RuntimeError("placement.tsv is required before collecting step metrics")
    with placement_path.open(encoding="utf-8") as stream:
        placement = list(csv.DictReader(stream, delimiter="\t"))

    ordered = sorted(placement, key=lambda row: (
        row.get("phase", ""), row.get("task", ""), row.get("started_at", ""), row.get("pod", "")
    ))
    attempt_by_task: dict[tuple[str, str], int] = defaultdict(int)
    rows: list[dict[str, object]] = []
    numeric_fields = (
        "logical_read_bytes", "logical_write_bytes", "storage_read_bytes",
        "storage_write_bytes", "elapsed_seconds", "wait_seconds", "compute_seconds", "windows",
        "transfer_seconds", "transfer_read_bytes", "transfer_write_bytes", "transfers",
    )
    window_rows: list[dict[str, object]] = []
    transfer_rows: list[dict[str, object]] = []
    for placement_row in ordered:
        key = (placement_row.get("phase", ""), placement_row.get("task", ""))
        attempt_by_task[key] += 1
        step_metric, windows, transfers = _pod_log_metrics(namespace, placement_row.get("pod", ""))
        metric = step_metric or {}
        pod_fields = {
            "phase": key[0], "task": key[1], "pod": placement_row.get("pod", ""),
            "node": placement_row.get("node", ""),
        }
        for window in windows:
            window_rows.append({
                **pod_fields, "window": window.get("window", ""),
                "wait_seconds": window.get("wait_seconds", ""),
                "compute_seconds": window.get("compute_seconds", ""),
                "transfer_seconds": window.get("transfer_seconds", ""),
                "transfer_read_bytes": window.get("transfer_read_bytes", ""),
                "transfer_write_bytes": window.get("transfer_write_bytes", ""),
                "started_at": window.get("started_at", ""), "ended_at": window.get("ended_at", ""),
            })
        for transfer in transfers:
            transfer_rows.append({
                **pod_fields, **{field: transfer.get(field, "") for field in TRANSFER_METRIC_FIELDS[4:]},
            })
        row: dict[str, object] = {
            "phase": key[0], "task": key[1], "pod": placement_row.get("pod", ""),
            "pod_uid": placement_row.get("pod_uid", ""), "node": placement_row.get("node", ""),
            "status": placement_row.get("status", ""), "attempt": attempt_by_task[key],
            "pod_elapsed_seconds": _elapsed_between(
                placement_row.get("started_at", ""), placement_row.get("finished_at", "")
            ),
            "started_at": placement_row.get("started_at", ""),
            "finished_at": placement_row.get("finished_at", ""),
        }
        for field in numeric_fields:
            row[field] = metric.get(field, "")
        rows.append(row)

    output = run_dir / "step-metrics.tsv"
    with output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=STEP_METRIC_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    windows_output = run_dir / "window-metrics.tsv"
    with windows_output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=WINDOW_METRIC_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(window_rows)

    transfers_output = run_dir / "transfer-metrics.tsv"
    with transfers_output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=TRANSFER_METRIC_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(transfer_rows)

    summary: dict[str, dict[str, object]] = {}
    for row in rows:
        key = f"{row['phase']}/{row['task']}"
        item = summary.setdefault(key, {
            "phase": row["phase"], "task": row["task"], "attempts": 0,
            "measured_attempts": 0, "logical_read_bytes": 0, "logical_write_bytes": 0,
            "storage_read_bytes": 0, "storage_write_bytes": 0,
            "cumulative_elapsed_seconds": 0.0, "cumulative_pod_elapsed_seconds": 0.0,
            "windows": 0, "cumulative_wait_seconds": 0.0, "cumulative_compute_seconds": 0.0,
            "transfers": 0, "cumulative_transfer_seconds": 0.0,
            "transfer_read_bytes": 0, "transfer_write_bytes": 0,
        })
        if row["transfer_seconds"] != "":
            item["transfers"] = int(item["transfers"]) + int(row["transfers"] or 0)
            item["cumulative_transfer_seconds"] = round(
                float(item["cumulative_transfer_seconds"]) + float(row["transfer_seconds"]), 6
            )
            for field in ("transfer_read_bytes", "transfer_write_bytes"):
                item[field] = int(item[field]) + int(row[field] or 0)
        if row["compute_seconds"] != "":
            item["windows"] = int(item["windows"]) + int(row["windows"] or 0)
            item["cumulative_wait_seconds"] = round(
                float(item["cumulative_wait_seconds"]) + float(row["wait_seconds"] or 0), 6
            )
            item["cumulative_compute_seconds"] = round(
                float(item["cumulative_compute_seconds"]) + float(row["compute_seconds"]), 6
            )
        item["attempts"] = int(item["attempts"]) + 1
        if row["elapsed_seconds"] != "":
            item["measured_attempts"] = int(item["measured_attempts"]) + 1
        for field in ("logical_read_bytes", "logical_write_bytes", "storage_read_bytes", "storage_write_bytes"):
            if row[field] != "":
                item[field] = int(item[field]) + int(row[field])
        if row["elapsed_seconds"] != "":
            item["cumulative_elapsed_seconds"] = round(
                float(item["cumulative_elapsed_seconds"]) + float(row["elapsed_seconds"]), 6
            )
        if row["pod_elapsed_seconds"] != "":
            item["cumulative_pod_elapsed_seconds"] = round(
                float(item["cumulative_pod_elapsed_seconds"]) + float(row["pod_elapsed_seconds"]), 6
            )
    (run_dir / "step-metrics-summary.json").write_text(
        json.dumps({"steps": summary}, indent=2), encoding="utf-8"
    )
    write_compute_normalized(run_dir)
    write_transfer_energy(run_dir)


def _as_float(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def write_compute_normalized(run_dir: Path) -> None:
    """Relate each Pod's energy to the time it spent computing rather than waiting.

    Needs both step-metrics.tsv (wait/compute seconds) and at least one energy summary. It is
    called after either is produced, so the order in which they are collected does not matter.
    compute_energy_j is an estimate: it assumes the Pod drew the same average power while
    waiting as while computing, which over-credits waiting (an idle poll loop draws less). The
    exact split needs the power time series intersected with window-metrics.tsv.
    """
    steps_path = run_dir / "step-metrics.tsv"
    summary_paths = energy_summary_paths(run_dir)
    if not steps_path.exists() or not summary_paths:
        return
    with steps_path.open(encoding="utf-8") as stream:
        steps = {row["pod"]: row for row in csv.DictReader(stream, delimiter="\t") if row.get("pod")}
    providers: dict[str, object] = {}
    for path in summary_paths:
        summary = json.loads(path.read_text(encoding="utf-8"))
        provider = summary.get("provider", "ecofloc")
        by_pod = summary.get("by_workload_pod_j" if provider == "alumet" else "by_pod_j", {})
        pods = []
        for pod, energy in sorted(by_pod.items()):
            step = steps.get(pod, {})
            wait, compute = _as_float(step.get("wait_seconds")), _as_float(step.get("compute_seconds"))
            energy = float(energy)
            entry: dict[str, object] = {
                "pod": pod, "phase": step.get("phase", ""), "task": step.get("task", ""),
                "energy_j": round(energy, 6),
                "pod_elapsed_seconds": _as_float(step.get("pod_elapsed_seconds")),
                "wait_seconds": wait, "compute_seconds": compute,
                "compute_fraction": None, "compute_energy_j": None, "energy_per_compute_second_j": None,
            }
            if wait is not None and compute is not None and wait + compute > 0:
                entry["compute_fraction"] = round(compute / (wait + compute), 6)
                entry["compute_energy_j"] = round(energy * compute / (wait + compute), 6)
                if compute > 0:
                    entry["energy_per_compute_second_j"] = round(energy / compute, 6)
            pods.append(entry)
        measured = [entry for entry in pods if entry["compute_seconds"] is not None]
        providers[provider] = {
            "pods": pods,
            "measured_pods": len(measured),
            "energy_j": round(sum(float(entry["energy_j"]) for entry in measured), 6),
            "compute_seconds": round(sum(float(entry["compute_seconds"]) for entry in measured), 6),
            "wait_seconds": round(sum(float(entry["wait_seconds"]) for entry in measured), 6),
            "compute_energy_j": round(sum(float(entry["compute_energy_j"] or 0) for entry in measured), 6),
        }
    (run_dir / "energy" / "compute-normalized.json").write_text(
        json.dumps({
            "providers": providers,
            "note": (
                "wait_seconds: blocked on a peer (input buffer, checkpoint ack, downstream ack). "
                "compute_seconds: all other time, including startup. compute_energy_j estimates "
                "the compute share of a Pod's energy assuming equal average power while waiting "
                "and computing; energy_per_compute_second_j = energy_j / compute_seconds. "
                "Pods without window metrics (for example the BERT stages) are listed with null "
                "compute fields and are excluded from the provider totals."
            ),
        }, indent=2),
        encoding="utf-8",
    )


ALUMET_MODULE = Path(__file__).resolve().parent / "alumet" / "alumet.py"
TRANSFER_ENERGY_FIELDS = TRANSFER_METRIC_FIELDS + (
    "node_energy_j", "node_excess_energy_j", "pod_attributed_energy_j",
    "network_bytes", "nfs_server_node", "nfs_server_energy_j",
)


def _alumet_module():
    spec = importlib.util.spec_from_file_location("eaer_alumet", ALUMET_MODULE)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _epoch(value: str) -> float | None:
    """Parse an RFC 3339 time (InfluxDB writes nanoseconds; datetime keeps microseconds)."""
    if not value:
        return None
    text = re.sub(r"(\.\d{6})\d+", r"\1", value.strip().replace("Z", "+00:00"))
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


class _Series:
    """Per-interval samples (energy in J, or byte deltas): sample i covers (t[i-1], t[i]]."""

    def __init__(self, points: list[tuple[float, float]]):
        points.sort()
        self.times = [t for t, _ in points]
        self.values = [v for _, v in points]
        gaps = sorted(b - a for a, b in zip(self.times, self.times[1:]) if b > a)
        # The first sample has no predecessor; assume the typical poll interval.
        self.first_width = gaps[len(gaps) // 2] if gaps else 1.0
        self.max_width = max(gaps[-1] if gaps else 0.0, self.first_width)

    def between(self, start: float, end: float) -> float:
        """Share of the samples overlapping [start, end], assuming uniform rate per sample."""
        total = 0.0
        first = bisect.bisect_right(self.times, start)
        last = bisect.bisect_right(self.times, end + self.max_width)
        for index in range(first, last):
            right = self.times[index]
            left = self.times[index - 1] if index else right - self.first_width
            width = right - left
            if width <= 0:
                continue
            overlap = min(end, right) - max(start, left)
            if overlap > 0:
                total += self.values[index] * overlap / width
        return total


def _rapl_domains(domains: set[str]) -> set[str]:
    """Same non-overlapping choice as alumet.rapl_components: platform, else package+dram."""
    for name in ("platform_total", "platform"):
        if name in domains:
            return {name}
    chosen = {
        next((name for name in names if name in domains), "")
        for names in (("package_total", "package"), ("dram_total", "dram"))
    } - {""}
    return chosen or domains


def _alumet_series(raw: str) -> dict[str, dict]:
    """Group Alumet's raw export into node RAPL, pod-attributed energy and NIC byte series."""
    alumet = _alumet_module()
    rapl: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    attributed: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    network: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for row in alumet.influx_rows(raw):
        metric = row.get("_measurement", "")
        if row.get("_field", "") not in {"", "value"}:
            continue
        moment = _epoch(row.get("_time", ""))
        try:
            value = float(row.get("_value", ""))
        except ValueError:
            continue
        if moment is None or not math.isfinite(value):
            continue
        name = metric.lower()
        node = alumet.row_node(row)
        consumer_kind = row.get("resource_consumer_kind") or row.get("consumer_kind", "")
        if name.startswith("network_bytes"):
            label = f"{row.get('interface', 'unknown')}/{row.get('direction', 'unknown')}"
            network[node][label].append((moment, value))
        elif "energy" not in name:
            continue
        elif "attributed" in name or consumer_kind not in {"", "local_machine"}:
            pod = next((row.get(key, "") for key in ("name", "pod", "pod_name", "k8s_pod_name") if row.get(key)), "")
            if pod:
                attributed[pod][metric].append((moment, value * alumet.joule_factor(metric)))
        elif name.startswith("rapl_"):
            rapl[node][row.get("domain", "unknown").lower()].append((moment, value * alumet.joule_factor(metric)))
    return {
        "rapl": {
            node: [_Series(domains[domain]) for domain in _rapl_domains(set(domains))]
            for node, domains in rapl.items()
        },
        "attributed": {
            pod: [_Series(points) for points in metrics.values()] for pod, metrics in attributed.items()
        },
        "network": {
            node: {label: _Series(points) for label, points in labels.items()}
            for node, labels in network.items()
        },
    }


def write_transfer_energy(run_dir: Path) -> None:
    """Estimate the energy of every inter-stage transfer from Alumet's power time series.

    Needs transfer-metrics.tsv and energy/alumet-raw.csv; like write_compute_normalized it is
    called after either is produced. Alumet has no NFS-specific probe, so each transfer gets
    bounds rather than one exact figure:
      node_energy_j            RAPL energy of the Pod's node during the transfer (upper bound:
                               includes everything else running on that node);
      pod_attributed_energy_j  Alumet's per-Pod attributed energy (lower bound: the NFS client
                               RPC work runs in kernel threads outside the Pod's cgroup);
      node_excess_energy_j     node energy above the node's idle power, when
                               energy/idle-power.json ({"node": watts}) exists;
      nfs_server_energy_j      RAPL energy of ERCTL_NFS_SERVER_NODE during a remote transfer.
    network_bytes is the node's non-loopback NIC traffic during the transfer (procfs plugin).
    Samples are spread uniformly over their poll interval, so sub-interval transfers get the
    interval's mean power. Concurrent transfers on one node share, and double count, it.
    """
    transfers_path = run_dir / "transfer-metrics.tsv"
    raw_path = run_dir / "energy" / "alumet-raw.csv"
    if not transfers_path.exists() or not raw_path.exists():
        return
    with transfers_path.open(encoding="utf-8") as stream:
        transfers = list(csv.DictReader(stream, delimiter="\t"))
    series = _alumet_series(raw_path.read_text(encoding="utf-8"))
    idle_path = run_dir / "energy" / "idle-power.json"
    idle_power = json.loads(idle_path.read_text(encoding="utf-8")) if idle_path.exists() else {}
    server = os.environ.get("ERCTL_NFS_SERVER_NODE", "")

    rows: list[dict[str, object]] = []
    edges: dict[str, dict[str, object]] = {}
    totals: dict[str, float] = defaultdict(float)
    for transfer in transfers:
        start, end = _epoch(transfer.get("started_at", "")), _epoch(transfer.get("ended_at", ""))
        if start is None or end is None:
            continue
        seconds = max(0.0, end - start)
        node = transfer.get("node", "")
        node_energy = sum(item.between(start, end) for item in series["rapl"].get(node, []))
        excess = None
        if node in idle_power:
            excess = max(0.0, node_energy - float(idle_power[node]) * seconds)
        pod_energy = sum(item.between(start, end) for item in series["attributed"].get(transfer.get("pod", ""), []))
        network = sum(
            item.between(start, end)
            for label, item in series["network"].get(node, {}).items()
            if not label.startswith("lo/")
        )
        server_energy = None
        if server and server != node:
            server_energy = sum(item.between(start, end) for item in series["rapl"].get(server, []))
        row = {field: transfer.get(field, "") for field in TRANSFER_METRIC_FIELDS}
        row.update({
            "node_energy_j": round(node_energy, 6),
            "node_excess_energy_j": "" if excess is None else round(excess, 6),
            "pod_attributed_energy_j": round(pod_energy, 6),
            "network_bytes": int(round(network)),
            "nfs_server_node": server if server_energy is not None else "",
            "nfs_server_energy_j": "" if server_energy is None else round(server_energy, 6),
        })
        rows.append(row)

        key = f"{transfer.get('phase', '')}/{transfer.get('task', '')} {transfer.get('direction', '')} {transfer.get('kind', '')}"
        edge = edges.setdefault(key, {
            "phase": transfer.get("phase", ""), "task": transfer.get("task", ""),
            "direction": transfer.get("direction", ""), "kind": transfer.get("kind", ""),
            "nodes": [], "transfers": 0, "bytes": 0, "seconds": 0.0, "node_energy_j": 0.0,
            "node_excess_energy_j": None, "pod_attributed_energy_j": 0.0, "network_bytes": 0,
            "nfs_server_energy_j": None,
        })
        if node and node not in edge["nodes"]:
            edge["nodes"].append(node)
        edge["transfers"] = int(edge["transfers"]) + 1
        edge["bytes"] = int(edge["bytes"]) + int(float(transfer.get("bytes") or 0))
        edge["network_bytes"] = int(edge["network_bytes"]) + int(row["network_bytes"])
        for field, value in (
            ("seconds", seconds), ("node_energy_j", node_energy), ("pod_attributed_energy_j", pod_energy),
            ("node_excess_energy_j", excess), ("nfs_server_energy_j", server_energy),
        ):
            if value is None:
                continue
            edge[field] = round(float(edge[field] or 0.0) + value, 6)
            totals[field] += value
        totals["transfers"] += 1
        totals["bytes"] += float(transfer.get("bytes") or 0)
        totals["network_bytes"] += float(row["network_bytes"])

    (run_dir / "energy").mkdir(parents=True, exist_ok=True)
    with (run_dir / "energy" / "transfer-energy.tsv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=TRANSFER_ENERGY_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    (run_dir / "energy" / "transfer-energy.json").write_text(json.dumps({
        "provider": "alumet",
        "nfs_server_node": server or None,
        "idle_power_w": idle_power,
        "totals": {
            key: int(value) if key in {"transfers", "bytes", "network_bytes"} else round(value, 6)
            for key, value in sorted(totals.items())
        },
        "by_edge": dict(sorted(edges.items())),
        "note": write_transfer_energy.__doc__.strip(),
    }, indent=2), encoding="utf-8")


def write_manifest(run_dir: Path, args: argparse.Namespace) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "manifest.json"
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    data.update({"run_id": run_dir.name, "status": args.status, "mode": args.mode})
    if args.archive_status:
        data["archive_status"] = args.archive_status
    # Record which result artifacts really exist on disk. `status` reflects the workload;
    # `artifacts` reflects what was saved -- the two can legitimately differ (e.g. the ER run
    # Succeeded but the matching-graph copy failed), and this makes that visible.
    data["artifacts"] = detect_artifacts(run_dir)
    data["updated_at"] = int(time.time())
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def pair_count(path: Path) -> int:
    with path.open(encoding="utf-8") as stream:
        return max(0, sum(1 for _ in stream) - 1)  # minus the CSV header


def energy_summary_paths(run_dir: Path) -> list[Path]:
    energy_dir = run_dir / "energy"
    paths = sorted(energy_dir.glob("*-summary.json"))
    old_path = energy_dir / "summary.json"
    return paths or ([old_path] if old_path.exists() else [])


def show(run_dir: Path) -> None:
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    phase_path = run_dir / "pipeline-phase.json"
    phase = json.loads(phase_path.read_text(encoding="utf-8")) if phase_path.exists() else {}
    offline_prep = (
        phase.get("phase") == "batch"
        and manifest.get("mode") == "embedding-training-inference-evaluation"
    )
    summary_paths = energy_summary_paths(run_dir)
    summaries = [json.loads(path.read_text(encoding="utf-8")) for path in summary_paths]
    matching = run_dir / "matching"
    graph = next((matching / "predicted").rglob("predicted_matching.csv"), None) if (matching / "predicted").exists() else None
    report = next((matching / "communication").rglob("evaluation_report.json"), None) if (matching / "communication").exists() else None

    status = manifest.get("status", "unknown")
    print(f"Run: {run_dir.name}  status={status}  mode={manifest.get('mode', '-')}")
    if manifest.get("archive_status"):
        print(f"Archive: {manifest['archive_status']}")
    if graph:
        print(f"Matching: {pair_count(graph)} pairs  file={graph}")
    elif offline_prep:
        print("Matching: not applicable (offline batch preparation; no evaluation)")
    else:
        print("Matching: (not collected)")
    if report:
        print(f"Evaluation: {report.read_text(encoding='utf-8').strip()}")
    # A Succeeded run with missing matching artifacts is an honest caveat, not a lie.
    if status == "Succeeded" and not offline_prep and not (graph and report):
        print("Note: workload Succeeded but some matching artifacts were not saved (see manifest.artifacts).")
    plan_path = run_dir / "scheduling-plan.tsv"
    if plan_path.exists():
        with plan_path.open(encoding="utf-8") as stream:
            plan = list(csv.DictReader(stream, delimiter="\t"))
        tasks: dict[tuple[str, str, str, str], dict[str, list[str]]] = {}
        for row in plan:
            key = (row["phase"], row["task"], row["strategies"], row["weights"])
            roles = tasks.setdefault(key, defaultdict(list))
            roles[row["role"]].append(row["node"])
        print(f"Scheduling plan: {len(tasks)} task(s)  file={plan_path}")
        for (phase, task, strategies, weights), roles in tasks.items():
            preferred = roles.get("preferred") or roles.get("allowed") or []
            detail = f"prefer={','.join(preferred) or '-'}"
            if roles.get("fallback"):
                detail += f" fallback={','.join(roles['fallback'])}"
            print(f"  {phase:<11} {task:<34} [{strategies} {weights}] {detail}")
    placement_path = run_dir / "placement.tsv"
    if placement_path.exists():
        with placement_path.open(encoding="utf-8") as stream:
            placement = list(csv.DictReader(stream, delimiter="\t"))
        print(f"Placement: {len(placement)} pod(s)  file={placement_path}")
        for row in placement:
            task = row["task"] or task_from_pod_name(row["pod"], row.get("workflow", ""))
            print(f"  {row['phase']:<11} {task:<34} -> {row['node'] or '(unscheduled)'}")
    metrics_summary_path = run_dir / "step-metrics-summary.json"
    if metrics_summary_path.exists():
        metrics = json.loads(metrics_summary_path.read_text(encoding="utf-8")).get("steps", {})
        print(f"Step metrics: {len(metrics)} step(s)  file={metrics_summary_path}")
        for item in metrics.values():
            measured = int(item.get("measured_attempts", 0))
            attempts = int(item.get("attempts", 0))
            if measured:
                io_text = (
                    f"read={item.get('logical_read_bytes', 0)}B "
                    f"write={item.get('logical_write_bytes', 0)}B "
                    f"storage-read={item.get('storage_read_bytes', 0)}B "
                    f"storage-write={item.get('storage_write_bytes', 0)}B "
                    f"cumulative={item.get('cumulative_elapsed_seconds', 0):.3f}s"
                )
            else:
                io_text = "I/O=n/a cumulative=n/a"
            time_text = ""
            if int(item.get("windows", 0)) or float(item.get("cumulative_compute_seconds", 0)):
                time_text = (
                    f" windows={item.get('windows', 0)}"
                    f" compute={item.get('cumulative_compute_seconds', 0):.3f}s"
                    f" wait={item.get('cumulative_wait_seconds', 0):.3f}s"
                )
            if int(item.get("transfers", 0)):
                time_text += (
                    f" transfer={item.get('cumulative_transfer_seconds', 0):.3f}s"
                    f" ({item.get('transfer_read_bytes', 0)}B in, {item.get('transfer_write_bytes', 0)}B out)"
                )
            print(
                f"  {item.get('phase', ''):<11} {item.get('task', ''):<34} "
                f"{io_text}{time_text} attempts={measured}/{attempts}"
            )
    for summary in summaries:
        provider = summary.get("provider", "ecofloc")
        if provider == "alumet":
            print(
                f"Energy (alumet): {summary.get('measurement_status', 'unknown')}  "
                f"hardware={summary.get('hardware_energy_j', 0):.3f} J"
            )
            print(
                "Attributed: "
                f"EAER={summary.get('workload_attributed_energy_j', 0):.3f} J  "
                f"system={summary.get('system_attributed_energy_j', 0):.3f} J  "
                f"unknown={summary.get('unknown_attributed_energy_j', 0):.3f} J  "
                f"all={summary.get('attributed_energy_j', 0):.3f} J"
            )
            if summary.get("uncovered_nodes"):
                print("Uncovered nodes: " + ", ".join(summary["uncovered_nodes"]))
        else:
            print(
                f"Energy ({provider}): {summary.get('measurement_status', 'unknown')}  "
                f"total={summary.get('total_energy_j', 0):.3f} J"
            )
        if provider == "ecofloc":
            by_pod_metric = summary.get("by_pod_metric_j", {})
            by_pod = summary.get("by_pod_j", {})
            task_by_pod = {
                row.get("pod", ""): row.get("task", "")
                for row in summary.get("sessions", [])
                if row.get("pod") and row.get("task")
            }
            if by_pod_metric:
                for pod, metrics in by_pod_metric.items():
                    label = pod
                    if POD_UID_PATTERN.fullmatch(pod) and task_by_pod.get(pod):
                        task = Path(task_by_pod[pod]).stem.lower().replace("_", "-")
                        label = f"{task}-{pod[:8]}"
                    components = "  ".join(
                        f"{metric.upper()}="
                        + (f"{metrics[metric]:.3f}J" if metric in metrics else "n/a")
                        for metric in ECOFLOC_METRICS
                    )
                    total = f"{by_pod.get(pod, 0):.3f}J" if metrics else "n/a"
                    print(f"  {label}: {components}  total={total}")
            else:
                for task, value in summary.get("by_task_j", {}).items():
                    print(f"  {task:<32} {value:.3f} J")
        else:
            by_stage_hardware = summary.get("by_stage_hardware_j", {})
            if by_stage_hardware:
                for stage, devices in by_stage_hardware.items():
                    components = "  ".join(
                        f"{device.upper()}={value:.3f}J"
                        for device, value in devices.items()
                    )
                    print(f"  {stage}: {components}  total={sum(devices.values()):.3f}J")
            else:
                for task, value in summary.get("by_task_j", {}).items():
                    print(f"  {task:<32} {value:.3f} J")
        by_metric = summary.get("by_metric_j", {})
        if by_metric:
            print("By metric: " + "  ".join(f"{metric}={value:.3f}J" for metric, value in by_metric.items()))
    normalized_path = run_dir / "energy" / "compute-normalized.json"
    if normalized_path.exists():
        normalized = json.loads(normalized_path.read_text(encoding="utf-8")).get("providers", {})
        for provider, data in normalized.items():
            print(
                f"Energy per compute second ({provider}): {data['measured_pods']} pod(s)  "
                f"compute={data['compute_seconds']:.3f}s wait={data['wait_seconds']:.3f}s  "
                f"compute-share={data['compute_energy_j']:.3f}J of {data['energy_j']:.3f}J"
            )
            for entry in data["pods"]:
                if entry["compute_seconds"] is None:
                    continue
                rate = entry["energy_per_compute_second_j"]
                print(
                    f"  {entry['task'] or entry['pod']:<34} energy={entry['energy_j']:.3f}J "
                    f"compute={entry['compute_seconds']:.3f}s wait={entry['wait_seconds']:.3f}s "
                    f"J/compute-s={'n/a' if rate is None else f'{rate:.3f}'}"
                )
    _show_transfer_energy(run_dir)


def _show_transfer_energy(run_dir: Path) -> None:
    path = run_dir / "energy" / "transfer-energy.json"
    if not path.exists():
        return
    data = json.loads(path.read_text(encoding="utf-8"))
    totals = data.get("totals", {})
    print(
        f"Transfer energy (alumet): {totals.get('transfers', 0)} transfer(s)  "
        f"{totals.get('bytes', 0)}B in {totals.get('seconds', 0):.3f}s  "
        f"node={totals.get('node_energy_j', 0):.3f}J pod-attributed={totals.get('pod_attributed_energy_j', 0):.3f}J"
        + (f" excess={totals['node_excess_energy_j']:.3f}J" if "node_excess_energy_j" in totals else "")
        + (f" nfs-server={totals['nfs_server_energy_j']:.3f}J" if "nfs_server_energy_j" in totals else "")
    )
    for key, edge in data.get("by_edge", {}).items():
        print(
            f"  {key:<52} n={edge['transfers']} {edge['bytes']}B {edge['seconds']:.3f}s "
            f"node={edge['node_energy_j']:.3f}J pod={edge['pod_attributed_energy_j']:.3f}J "
            f"nic={edge['network_bytes']}B on {','.join(edge['nodes']) or '-'}"
        )


def resolve_run(root: Path, name: str) -> Path:
    if not root.exists():
        raise SystemExit("No saved results")
    runs = sorted((path for path in root.iterdir() if path.is_dir()), key=lambda path: path.stat().st_mtime)
    if name == "latest":
        if not runs:
            raise SystemExit("No saved results")
        return runs[-1]
    return root / name


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    energy = sub.add_parser("energy")
    energy.add_argument("run_dir", type=Path)
    derived = sub.add_parser("derived")
    derived.add_argument("run_dir", type=Path)
    collect = sub.add_parser("collect")
    collect.add_argument("run_dir", type=Path)
    collect.add_argument("--namespace", default="argo")
    collect.add_argument("--local-root", default="")
    collect.add_argument("--node", default="")
    metrics = sub.add_parser("metrics")
    metrics.add_argument("run_dir", type=Path)
    metrics.add_argument("--namespace", default="argo")
    placement = sub.add_parser("placement")
    placement.add_argument("run_dir", type=Path)
    placement.add_argument("--namespace", default="argo")
    placement.add_argument("--phase", choices=("batch", "incremental"), required=True)
    placement.add_argument("--workflow", default="")
    manifest = sub.add_parser("manifest")
    manifest.add_argument("run_dir", type=Path)
    manifest.add_argument("--status", required=True)
    manifest.add_argument("--mode", default="")
    manifest.add_argument("--archive-status", choices=("succeeded", "failed"))
    listing = sub.add_parser("list")
    listing.add_argument("--root", type=Path, required=True)
    display = sub.add_parser("show")
    display.add_argument("--root", type=Path, required=True)
    display.add_argument("--run", default="latest")
    args = parser.parse_args()

    if args.command == "energy":
        try:
            energy_summary(args.run_dir)
        except RuntimeError as error:
            raise SystemExit(str(error)) from None
    elif args.command == "derived":
        # Run after every provider has written its summary: `metrics` runs earlier in the
        # pipeline, before any energy file exists.
        write_compute_normalized(args.run_dir)
        write_transfer_energy(args.run_dir)
    elif args.command == "collect":
        collect_matching(args.run_dir, args.namespace, args.local_root, args.node)
    elif args.command == "metrics":
        collect_step_metrics(args.run_dir, args.namespace)
    elif args.command == "placement":
        collect_placement(args.run_dir, args.namespace, args.phase, args.workflow)
    elif args.command == "manifest":
        write_manifest(args.run_dir, args)
    elif args.command == "list":
        for path in sorted(args.root.glob("*")):
            if path.is_dir():
                print(path.name)
    else:
        show(resolve_run(args.root, args.run))


if __name__ == "__main__":
    main()
