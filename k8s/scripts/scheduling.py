#!/usr/bin/env python3
"""Inspect placement decisions and optionally apply H1/H2 Job migration."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import yaml

from scheduler import rank_nodes


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, check=check)


def load_config(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as stream:
        config = yaml.safe_load(stream) or {}
    if config.get("version") != 2:
        raise ValueError("runtime scheduling requires scheduling.yaml version: 2")
    return config


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
    if not eligible or current not in {item.node for item in eligible}:
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
    move = Path(__file__).with_name("move.py")
    result = subprocess.run([
        sys.executable, str(move), args.task, f"--to={best.node}", "--namespace", args.namespace
    ])
    return result.returncode


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
    adapt.add_argument("--apply", action="store_true", help="recreate the Job on the selected node")
    adapt.add_argument("--metrics", help="YAML/JSON node metrics (e.g. power_watts for H1)")
    adapt.add_argument("--watch", type=int, metavar="SECONDS", help="repeat at this interval")
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
