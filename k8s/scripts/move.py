#!/usr/bin/env python3
"""Recreate one incremental Job with required affinity to a target node.

DEPRECATED: this manual one-off pinning tool is superseded by the scheduler
(`erctl schedule adapt <task>`), which chooses and applies placement from the
strategies in scheduling.yaml. Kept for ad-hoc manual moves only.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


def kubectl(*args: str, check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["kubectl", *args], text=True, capture_output=True, check=check)


def find_pod(name: str, namespace: str) -> dict[str, Any]:
    exact = kubectl("get", "pod", name, "-n", namespace, "-o", "json")
    if exact.returncode == 0:
        return json.loads(exact.stdout)
    result = kubectl("get", "pods", "-n", namespace, "-l", f"app={name}", "-o", "json")
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or f"Pod for task '{name}' not found")
    items = json.loads(result.stdout).get("items", [])
    running = [item for item in items if item.get("status", {}).get("phase") == "Running"]
    if not running:
        raise RuntimeError(f"No Running pod found for task '{name}'")
    return running[0]


def owner_job(pod: dict[str, Any]) -> str:
    for owner in pod.get("metadata", {}).get("ownerReferences", []):
        if owner.get("kind") == "Job" and owner.get("name"):
            return str(owner["name"])
    raise RuntimeError("Pod is not owned by a Kubernetes Job")


def find_manifest(job_name: str) -> tuple[Path, dict[str, Any]]:
    root = Path(__file__).resolve().parent.parent / "pipeline" / "exec" / "incremental"
    for path in root.rglob("*.yaml"):
        with path.open(encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
        if isinstance(document, dict) and document.get("kind") == "Job":
            if document.get("metadata", {}).get("name") == job_name:
                return path, document
    raise RuntimeError(f"Generated manifest for Job '{job_name}' not found; run erctl compile")


def pin_job(job: dict[str, Any], node: str) -> None:
    pod_spec = job["spec"]["template"]["spec"]
    affinity = pod_spec.setdefault("affinity", {})
    node_affinity = affinity.setdefault("nodeAffinity", {})
    node_affinity["requiredDuringSchedulingIgnoredDuringExecution"] = {
        "nodeSelectorTerms": [{
            "matchFields": [{"key": "metadata.name", "operator": "In", "values": [node]}]
        }]
    }


def move(name: str, target: str, namespace: str) -> None:
    if kubectl("get", "node", target).returncode:
        raise RuntimeError(f"Node '{target}' not found")
    pod = find_pod(name, namespace)
    current = pod.get("spec", {}).get("nodeName")
    if current == target:
        raise RuntimeError(f"Pod is already on node '{target}'")
    job_name = owner_job(pod)
    manifest_path, job = find_manifest(job_name)
    pin_job(job, target)
    temporary = Path("/tmp") / f"{job_name}-moved.yaml"
    with temporary.open("w", encoding="utf-8") as stream:
        yaml.safe_dump(job, stream, sort_keys=False)
    try:
        deleted = kubectl("delete", "job", job_name, "-n", namespace)
        if deleted.returncode:
            raise RuntimeError(deleted.stderr.strip())
        applied = kubectl("apply", "-f", str(temporary), "-n", namespace)
        if applied.returncode:
            raise RuntimeError(applied.stderr.strip())
    finally:
        temporary.unlink(missing_ok=True)
    print(f"Moved Job {job_name} from {current} to {target} using {manifest_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Move an incremental Job between nodes")
    parser.add_argument("pod", help="Task label or exact pod name")
    parser.add_argument("--to", dest="target_node", required=True)
    parser.add_argument("--namespace", default="argo")
    args = parser.parse_args()
    print(
        "[DEPRECATED] erctl move is deprecated; prefer 'erctl schedule adapt <task>'.",
        file=sys.stderr,
    )
    try:
        move(args.pod, args.target_node, args.namespace)
    except (OSError, KeyError, RuntimeError, yaml.YAMLError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
