#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import copy
import sys
from pathlib import Path
from typing import Any
import yaml

def pod_exists(pod_name: str, namespace: str) -> bool:
    command = f"kubectl get pod {pod_name} -n {namespace}"
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result is None:
        print("kubectl is not installed or not found in PATH.")
        sys.exit(1)
    return result.returncode == 0

def node_exists(node_name: str) -> bool:
    command = f"kubectl get node {node_name}"
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result is None:
        print("kubectl is not installed or not found in PATH.")
        sys.exit(1)
    return result.returncode == 0

def get_actual_pod_name(pod_name: str, namespace: str) -> str:
    command = f"kubectl get pods -n {namespace} --no-headers -o custom-columns=NAME:.metadata.name"
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result is None:
        print("kubectl is not installed or not found in PATH.")
        sys.exit(1)
    pod_names = result.stdout.decode().strip().splitlines()
    for name in pod_names:
        if name.startswith(pod_name):
            return name
    print(f"Could not find actual pod name for {pod_name} in namespace {namespace}.")
    sys.exit(1)

def pod_already_on_node(pod_name: str, node_name: str, namespace: str) -> bool:
    command = f"kubectl get pod {pod_name} -n {namespace} -o jsonpath='{{.spec.nodeName}}'"
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result is None:
        print("kubectl is not installed or not found in PATH.")
        sys.exit(1)
    current_node = result.stdout.decode().strip()
    return current_node == node_name

def edit_pod_manifest(pod_name: str, target_node: str, namespace: str) -> None:
    base_pod_name = pod_name.replace("-", "_")
    base_pod_name = base_pod_name.rsplit("_", 1)[0]

    # Get the current pod manifest recursively
    manifests_dir = Path(__file__).parent.parent / "pipeline" / "exec" 
    command = f"find {manifests_dir} -name '{base_pod_name}.yaml'"
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 0:
        print(f"Could not find manifest '{base_pod_name}.yaml' for pod {pod_name} in {manifests_dir}.")
        sys.exit(1)
    
    manifest_path = result.stdout.decode().strip()
    print(f"Found manifest for pod {pod_name}: {manifest_path}")

    with open(manifest_path, "r") as f:
        pod_manifest = yaml.safe_load(f)
        # Copy the manifest to avoid modifying the original
        pod_manifest = copy.deepcopy(pod_manifest)
        pod_manifest["spec"]["template"]["spec"]["affinity"] = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchFields": [
                                {
                                    "key": "metadata.name",
                                    "operator": "In",
                                    "values": [target_node]
                                }
                            ]
                        }
                    ]
                }
            }
        }
        # Save the modified manifest back to a temp file
        new_manifest_path = Path(manifest_path).with_name(f"{base_pod_name}_moved.yaml")
        yaml.dump(pod_manifest, open(new_manifest_path, "w"))
        print(f"Modified manifest saved to {new_manifest_path}. Moving pod {pod_name} to node {target_node}.")
        # Delete the existing job
        job_name = base_pod_name.replace("_", "-")
        delete_command = f"kubectl delete job {job_name} -n {namespace}"
        delete_result = subprocess.run(delete_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if delete_result.returncode != 0:
            print(f"Failed to delete job {job_name}. Error: {delete_result.stderr.decode()}")
            sys.exit(1)
        print(f"Deleted job {job_name}.")
        # Apply the modified manifest to move the pod
        apply_command = f"kubectl apply -f {new_manifest_path} -n {namespace}"
        apply_result = subprocess.run(apply_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if apply_result.returncode != 0:
            print(f"Failed to apply modified manifest for pod {pod_name}. Error: {apply_result.stderr.decode()}")
            sys.exit(1)
        print(f"Successfully moved pod {pod_name} to node {target_node}.")
        # Clean up the temporary manifest file
        new_manifest_path.unlink()

def main() -> None:
    parser = argparse.ArgumentParser(description="Move pods between nodes during execution.")
    parser.add_argument(
        "pod",
        help="The name of the pod to move.",
    )

    parser.add_argument(
        "--to=",
        dest="target_node",
        required=True,
        help="The name of the target node to which the pod should be moved.",
    )

    parser.add_argument(
        "--namespace",
        default="argo",
        help="The namespace in which the pods are running. Default is 'default'.",
    )
    args = parser.parse_args()
    
    pod = get_actual_pod_name(args.pod, args.namespace)

    if not pod_exists(pod, args.namespace):
        print(f"Pod {pod} not found in namespace {args.namespace}.")
        sys.exit(1)

    if not node_exists(args.target_node):
        print(f"Node {args.target_node} not found.")
        sys.exit(1)

    if pod_already_on_node(pod, args.target_node, args.namespace):
        print(f"Pod {pod} is already on node {args.target_node}.")
        sys.exit(1)

    edit_pod_manifest(pod, args.target_node, args.namespace)

if __name__ == "__main__":
    raise SystemExit(main())