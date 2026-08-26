#!/usr/bin/env python3
"""Persist and display matching, placement, evaluation, and monitoring results."""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path


INCREMENTAL_JOBS = {
    "calculating-similarity", "candidate-enumeration", "cg-feature-extraction",
    "decision-making", "embedding-training", "evaluation", "graph-construction",
    "kafka-consumer", "kafka-producer", "normalization", "random-walk",
}
PLACEMENT_FIELDS = (
    "phase", "task", "pod", "pod_uid", "node", "pod_ip", "status", "started_at",
    "finished_at", "workflow", "job",
)
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
        graph = next((matching / "predicted").rglob("predicted_matching.graphml"), None)
    if (matching / "communication").exists():
        report = next((matching / "communication").rglob("evaluation_report.json"), None)
    return {
        "energy_summary": any(energy_summary_paths(run_dir)),
        "matching_graph": graph is not None,
        "evaluation_report": report is not None,
        "scheduling_plan": (run_dir / "scheduling-plan.tsv").exists(),
        "pod_placement": (run_dir / "placement.tsv").exists(),
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


def collect_matching(run_dir: Path, namespace: str) -> None:
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
            "volumes": [
                {"name": "predicted", "persistentVolumeClaim": {"claimName": "pipeline-decision-evaluation-cache-claim"}},
                {"name": "communication", "persistentVolumeClaim": {"claimName": "pipeline-communication-claim"}},
            ],
        },
    }
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


def edge_count(path: Path) -> int:
    return sum(1 for element in ET.parse(path).iter() if element.tag.endswith("edge"))


def energy_summary_paths(run_dir: Path) -> list[Path]:
    energy_dir = run_dir / "energy"
    paths = sorted(energy_dir.glob("*-summary.json"))
    old_path = energy_dir / "summary.json"
    return paths or ([old_path] if old_path.exists() else [])


def show(run_dir: Path) -> None:
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    summary_paths = energy_summary_paths(run_dir)
    summaries = [json.loads(path.read_text(encoding="utf-8")) for path in summary_paths]
    matching = run_dir / "matching"
    graph = next((matching / "predicted").rglob("predicted_matching.graphml"), None) if (matching / "predicted").exists() else None
    report = next((matching / "communication").rglob("evaluation_report.json"), None) if (matching / "communication").exists() else None

    status = manifest.get("status", "unknown")
    print(f"Run: {run_dir.name}  status={status}  mode={manifest.get('mode', '-')}")
    if manifest.get("archive_status"):
        print(f"Archive: {manifest['archive_status']}")
    if graph:
        print(f"Matching: {edge_count(graph)} edges  file={graph}")
    else:
        print("Matching: (not collected)")
    if report:
        print(f"Evaluation: {report.read_text(encoding='utf-8').strip()}")
    # A Succeeded run with missing matching artifacts is an honest caveat, not a lie.
    if status == "Succeeded" and not (graph and report):
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
            for task, value in summary.get("by_task_j", {}).items():
                print(f"  {task:<32} {value:.3f} J")
        by_metric = summary.get("by_metric_j", {})
        if by_metric:
            print("By metric: " + "  ".join(f"{metric}={value:.3f}J" for metric, value in by_metric.items()))


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
    collect = sub.add_parser("collect")
    collect.add_argument("run_dir", type=Path)
    collect.add_argument("--namespace", default="argo")
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
    elif args.command == "collect":
        collect_matching(args.run_dir, args.namespace)
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
