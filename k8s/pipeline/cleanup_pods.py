"""Select old pipeline Pods, or reject PVC deletion while references remain."""

import argparse
import json
import sys


def inspect_pods(document, jobs, claims, verify=False):
    selected = []
    blockers = []
    for pod in document["items"]:
        metadata = pod["metadata"]
        name = metadata["name"]
        owned = name.startswith("pipeline-") or any(
            owner.get("kind") == "Job" and owner.get("name") in jobs
            for owner in metadata.get("ownerReferences", [])
        )
        references = {
            volume["persistentVolumeClaim"]["claimName"]
            for volume in pod.get("spec", {}).get("volumes", [])
            if "persistentVolumeClaim" in volume
        } & claims
        if verify and (owned or references):
            blockers.append(f"{name}: {', '.join(sorted(references)) or 'pipeline Pod still exists'}")
        elif owned:
            selected.append(f"pod/{name}")
    if blockers:
        raise ValueError("Refusing to delete PVCs; Pods remain:\n" + "\n".join(blockers))
    return selected


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", nargs="+", required=True)
    parser.add_argument("--claims", nargs="+", required=True)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    try:
        selected = inspect_pods(
            json.load(sys.stdin), set(args.jobs), set(args.claims), args.verify
        )
    except (ValueError, KeyError) as error:
        sys.exit(str(error))
    for reference in selected:
        print(reference)
