#!/usr/bin/env python3
"""Inspect placement decisions and optionally apply H1/H2 Job migration."""
from __future__ import annotations

import argparse
import copy
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import yaml

from placement_config import load_policy_config, prepare_policy_config
from scheduler import rank_nodes

MIGRATION_LABEL = "eaer.er/migration"
MIGRATION_MARKER_PREFIX = "eaer-hot-migration-"


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, check=check)


def load_config(path: Path) -> dict[str, Any]:
    # Resolve first (compiler.py already does): a relative --config must resolve its includes
    # and carbon-intensity.yaml next to the config file, not relative to the current directory.
    path = path.resolve()
    config = load_policy_config(path)
    return prepare_policy_config(
        config, path, Path(__file__).resolve().parent.parent / "results"
    )


def mapping(raw: Any, id_key: str) -> dict[str, dict[str, Any]]:
    if isinstance(raw, dict):
        return {str(key): (value or {}) for key, value in raw.items()}
    return {str(item[id_key]): item for item in (raw or [])}


def task_config(config: dict[str, Any], group: str, name: str) -> tuple[dict[str, Any], Any, dict[str, Any]]:
    section = config.get(group, {}) or {}
    defaults = section.get("defaults", {}) or {}
    raw = (section.get("templates", {}) or {}).get(name)
    if raw is None and name not in (section.get("templates", {}) or {}):
        raise ValueError(f"Unknown task '{name}' in {group}")
    raw = raw or {}
    task = dict(defaults.get("task", {}) or {})
    task.update(raw.get("task", {}) or {})
    task["task_id"] = name
    strategy = raw.get("strategy", defaults.get("strategy", config.get("strategy", "B0")))
    preferences = dict(config.get("preferences", {}) or {})
    preferences.update(defaults.get("preferences", {}) or {})
    preferences.update(raw.get("preferences", {}) or {})
    return task, strategy, preferences


def add_live_load(nodes: dict[str, dict[str, Any]]) -> None:
    result = run(["kubectl", "top", "nodes", "--no-headers"], check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "kubectl top nodes failed")
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) >= 5 and fields[0] in nodes:
            nodes[fields[0]]["cpu_utilization"] = fields[2]
            nodes[fields[0]]["memory_utilization"] = fields[4]


def add_metrics(nodes: dict[str, dict[str, Any]], path: str | None) -> None:
    """Merge a collector-produced YAML/JSON node mapping (not Kubernetes labels)."""
    if not path:
        return
    with Path(path).open(encoding="utf-8") as stream:
        raw = yaml.safe_load(stream) or {}
    updates = mapping(raw.get("nodes", raw) if isinstance(raw, dict) else raw, "node_id")
    for name, values in updates.items():
        if name in nodes:
            nodes[name].update(values)


def pod_for_task(task: str, namespace: str) -> dict[str, Any] | None:
    result = run([
        "kubectl", "get", "pods", "-n", namespace, "-l", f"app={task}", "-o", "json"
    ], check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    items = json.loads(result.stdout).get("items", [])
    running = [item for item in items if item.get("status", {}).get("phase") == "Running"]
    return running[0] if running else None


def job_manifest(pod: dict[str, Any]) -> tuple[str, dict[str, Any], Path]:
    job_name = next(
        (owner.get("name") for owner in pod.get("metadata", {}).get("ownerReferences", [])
         if owner.get("kind") == "Job" and owner.get("name")),
        None,
    )
    if not job_name:
        raise RuntimeError("Pod is not owned by a Kubernetes Job")

    manifests = Path(__file__).resolve().parent.parent / "pipeline" / "exec" / "incremental"
    job = None
    manifest_path = None
    for path in manifests.rglob("*.yaml"):
        with path.open(encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
        if (isinstance(document, dict) and document.get("kind") == "Job"
                and document.get("metadata", {}).get("name") == job_name):
            job, manifest_path = document, path
            break
    if job is None:
        raise RuntimeError(
            f"Generated manifest for Job '{job_name}' not found; run pipeline start first"
        )
    return job_name, job, manifest_path


def apply_document(document: dict[str, Any], namespace: str) -> None:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", encoding="utf-8") as stream:
        yaml.safe_dump(document, stream, sort_keys=False)
        stream.flush()
        result = run(["kubectl", "apply", "-f", stream.name, "-n", namespace], check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())


def marker_name(job_name: str) -> str:
    safe = re.sub(r"[^a-z0-9-]", "-", job_name.lower()).strip("-")
    return f"{MIGRATION_MARKER_PREFIX}{safe}"[:63].rstrip("-")


def set_migration_marker(job_name: str, source: str, target: str, namespace: str) -> str:
    name = marker_name(job_name)
    apply_document({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": name, "labels": {MIGRATION_LABEL: "true"}},
        "data": {"job": job_name, "source": source, "target": target},
    }, namespace)
    return name


def delete_migration_marker(name: str, namespace: str) -> None:
    run(["kubectl", "delete", "configmap", name, "-n", namespace,
         "--ignore-not-found=true", "--wait=false"], check=False)


def wait_for_pod(pod_name: str, namespace: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = run(["kubectl", "get", "pod", pod_name, "-n", namespace, "-o", "json"], check=False)
        if result.returncode == 0:
            pod = json.loads(result.stdout)
            phase = pod.get("status", {}).get("phase")
            if phase == "Succeeded":
                return
            if phase == "Failed":
                raise RuntimeError(f"image pre-pull Pod {pod_name} failed")
        time.sleep(1)
    raise RuntimeError(f"image pre-pull Pod {pod_name} did not finish within {timeout}s")


def pre_pull_image(job: dict[str, Any], task: str, node: str, namespace: str, timeout: int) -> None:
    template_spec = job["spec"]["template"]["spec"]
    container = template_spec["containers"][0]
    suffix = str(time.time_ns())[-8:]
    pod_name = f"eaer-prepull-{re.sub(r'[^a-z0-9-]', '-', task.lower())}-{suffix}"[:63].rstrip("-")
    pod_spec: dict[str, Any] = {
        "nodeName": node,
        "restartPolicy": "Never",
        "containers": [{
            "name": "pull",
            "image": container["image"],
            "imagePullPolicy": container.get("imagePullPolicy", "IfNotPresent"),
            "command": ["/bin/sh", "-c", "true"],
        }],
    }
    if template_spec.get("imagePullSecrets"):
        pod_spec["imagePullSecrets"] = copy.deepcopy(template_spec["imagePullSecrets"])
    if template_spec.get("serviceAccountName"):
        pod_spec["serviceAccountName"] = template_spec["serviceAccountName"]
    if template_spec.get("tolerations"):
        pod_spec["tolerations"] = copy.deepcopy(template_spec["tolerations"])
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "labels": {"app": "eaer-image-prepull"}},
        "spec": pod_spec,
    }
    print(f"pre-pull: {container['image']} on {node}")
    try:
        apply_document(pod, namespace)
        wait_for_pod(pod_name, namespace, timeout)
    finally:
        run(["kubectl", "delete", "pod", pod_name, "-n", namespace,
             "--ignore-not-found=true", "--wait=false"], check=False)


def request_window_boundary_stop(pod: dict[str, Any], namespace: str) -> None:
    pod_name = pod["metadata"]["name"]
    result = run([
        "kubectl", "exec", "-n", namespace, pod_name, "-c", "main", "--",
        "/bin/sh", "-c", "kill -TERM 1",
    ], check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or f"failed to signal Pod {pod_name}")


def wait_for_job_complete(job_name: str, namespace: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout if timeout > 0 else None
    while deadline is None or time.monotonic() < deadline:
        result = run(["kubectl", "get", "job", job_name, "-n", namespace, "-o", "json"], check=False)
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or f"Job {job_name} disappeared while draining")
        job = json.loads(result.stdout)
        conditions = {
            item.get("type"): item.get("status")
            for item in job.get("status", {}).get("conditions", [])
        }
        if conditions.get("Complete") == "True":
            return
        if conditions.get("Failed") == "True":
            raise RuntimeError(f"Job {job_name} failed while waiting for its mini-batch boundary")
        time.sleep(1)
    raise RuntimeError(f"Job {job_name} did not reach a mini-batch boundary within {timeout}s")


def wait_for_replacement(job_name: str, node: str, namespace: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = run([
            "kubectl", "get", "pods", "-n", namespace, "-l", f"job-name={job_name}",
            "-o", "json",
        ], check=False)
        if result.returncode == 0:
            for pod in json.loads(result.stdout).get("items", []):
                if pod.get("metadata", {}).get("deletionTimestamp"):
                    continue
                assigned = pod.get("spec", {}).get("nodeName")
                phase = pod.get("status", {}).get("phase")
                if phase == "Failed":
                    raise RuntimeError(f"replacement Pod for {job_name} failed on {assigned}")
                if assigned == node and phase in {"Running", "Succeeded"}:
                    return
        time.sleep(1)
    raise RuntimeError(f"replacement Job {job_name} was not running on {node} within {timeout}s")


def recreate_job_at_boundary(
    pod: dict[str, Any], node: str, namespace: str, pull_timeout: int, drain_timeout: int
) -> None:
    job_name, source_job, manifest_path = job_manifest(pod)
    current = pod.get("spec", {}).get("nodeName", "")
    replacement = copy.deepcopy(source_job)

    pod_spec = replacement["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    env = container.get("env") or []
    if not isinstance(env, list):
        raise RuntimeError(f"Job {job_name} container env must be a list")
    container["env"] = env
    hot_restart = next((item for item in env if item.get("name") == "EAER_HOT_RESTART"), None)
    if hot_restart is None:
        env.append({"name": "EAER_HOT_RESTART", "value": "true"})
    else:
        hot_restart["value"] = "true"
    affinity = pod_spec.setdefault("affinity", {}).setdefault("nodeAffinity", {})
    affinity["requiredDuringSchedulingIgnoredDuringExecution"] = {
        "nodeSelectorTerms": [{
            "matchFields": [{"key": "metadata.name", "operator": "In", "values": [node]}]
        }]
    }
    marker = set_migration_marker(job_name, current, node, namespace)
    try:
        pre_pull_image(source_job, job_name, node, namespace, pull_timeout)
        print(f"drain: waiting for {job_name} to finish its current mini-batch on {current}")
        request_window_boundary_stop(pod, namespace)
        wait_for_job_complete(job_name, namespace, drain_timeout)
        deleted = run([
            "kubectl", "delete", "job", job_name, "-n", namespace,
            "--wait=true", "--timeout=60s",
        ], check=False)
        if deleted.returncode:
            raise RuntimeError(deleted.stderr.strip() or f"failed to delete drained Job {job_name}")
        apply_document(replacement, namespace)
        wait_for_replacement(job_name, node, namespace, pull_timeout)
    finally:
        delete_migration_marker(marker, namespace)
    print(f"migrated Job {job_name}: {current} -> {node}; manifest={manifest_path}")


def recommend(
    config: dict[str, Any], group: str, name: str, live: bool, metrics: str | None = None
) -> list[Any]:
    nodes = mapping(config.get("nodes"), "node_id")
    add_metrics(nodes, metrics)
    if live:
        add_live_load(nodes)
    task, strategy, preferences = task_config(config, group, name)
    return rank_nodes(task, nodes, mapping(config.get("data"), "data_id"), strategy, preferences)


def show(ranking: list[Any]) -> None:
    for item in ranking:
        state = "eligible" if item.eligible else "blocked"
        print(f"{item.node:<48} score={item.score:.3f}  {state:<8} {item.reason}")


def adapt_once(config: dict[str, Any], args: argparse.Namespace) -> int:
    task, strategy, preferences = task_config(config, args.group, args.task)
    methods = [strategy] if isinstance(strategy, str) else strategy
    methods = [str(item).upper() for item in methods]
    if not set(methods) & {"H1", "H2"}:
        raise ValueError("adapt requires H1 or H2 (or a composition containing it)")
    if not task.get("migratable", False):
        print(f"skip {args.task}: task.migratable is false")
        return 0

    pod = pod_for_task(args.task, args.namespace)
    if not pod:
        print(f"skip {args.task}: no Running pod")
        return 0
    current = pod.get("spec", {}).get("nodeName")
    ranking = recommend(config, args.group, args.task, live="H2" in methods, metrics=args.metrics)
    eligible = [item for item in ranking if item.eligible]
    if not eligible:
        raise RuntimeError(f"No eligible node for {args.task}")
    if current not in {item.node for item in eligible}:
        current_score = 0.0
    else:
        current_score = next(item.score for item in eligible if item.node == current)
    best = eligible[0]
    improvement = best.score - current_score
    threshold = float(preferences.get("migration_threshold", 0.15))
    print(f"{args.task}: current={current}({current_score:.3f}) best={best.node}({best.score:.3f})")
    if best.node == current or improvement < threshold:
        print(f"keep: improvement {improvement:.3f} < threshold {threshold:.3f}")
        return 0
    if not args.apply:
        print(f"recommend move to {best.node}; add --apply to recreate the Job")
        return 0
    recreate_job_at_boundary(
        pod, best.node, args.namespace, args.pull_timeout, args.drain_timeout
    )
    return 0


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="EAER placement recommendation/runtime adaptation")
    parser.add_argument("--config", default=str(script_dir / "scheduling.yaml"))
    sub = parser.add_subparsers(dest="command", required=True)
    inspect = sub.add_parser("recommend", help="rank nodes for one configured task")
    inspect.add_argument("task")
    inspect.add_argument("--group", choices=("batch", "incremental"), default="incremental")
    inspect.add_argument("--live", action="store_true", help="merge kubectl top node load")
    inspect.add_argument("--metrics", help="YAML/JSON node metrics (e.g. power_watts for H1)")
    adapt = sub.add_parser("adapt", help="evaluate an H1/H2 task and optionally migrate it")
    adapt.add_argument("task")
    adapt.add_argument("--group", choices=("batch", "incremental"), default="incremental")
    adapt.add_argument("--namespace", default="argo")
    adapt.add_argument(
        "--apply", action="store_true",
        help="pre-pull the image, drain the current mini-batch, and move the Job",
    )
    adapt.add_argument("--metrics", help="YAML/JSON node metrics (e.g. power_watts for H1)")
    adapt.add_argument("--watch", type=int, metavar="SECONDS", help="repeat at this interval")
    adapt.add_argument("--pull-timeout", type=int, default=300, metavar="SECONDS")
    adapt.add_argument(
        "--drain-timeout", type=int, default=0, metavar="SECONDS",
        help="maximum mini-batch drain time; 0 waits until the boundary (default)",
    )
    args = parser.parse_args()
    try:
        config = load_config(Path(args.config))
        if args.command == "recommend":
            show(recommend(config, args.group, args.task, args.live, args.metrics))
            return 0
        while True:
            result = adapt_once(config, args)
            if result or not args.watch:
                return result
            time.sleep(max(1, args.watch))
    except (OSError, ValueError, RuntimeError, yaml.YAMLError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
