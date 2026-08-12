#!/usr/bin/env python3
"""Manage Alumet and capture one pipeline time window from its deployment."""
from __future__ import annotations

import argparse
import base64
import csv
import json
import math
import os
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


NAMESPACE = os.environ.get("ERCTL_ALUMET_NAMESPACE", "alumet")
ORG = os.environ.get("ERCTL_ALUMET_ORG", "influxdata")
BUCKET = os.environ.get("ERCTL_ALUMET_BUCKET", "default")
INFLUX_POD = os.environ.get("ERCTL_ALUMET_INFLUX_POD", "")
TOKEN_SECRET = os.environ.get("ERCTL_ALUMET_TOKEN_SECRET", "")
TOKEN_KEY = os.environ.get("ERCTL_ALUMET_TOKEN_KEY", "admin-token")
DRAIN_SECONDS = float(os.environ.get("ERCTL_ALUMET_DRAIN_SECONDS", "3"))
RETENTION = os.environ.get("ERCTL_ALUMET_RETENTION", "7d")
ENABLE_LABEL = "eaer.alumet/enabled"


def kubectl(*args: str, input_text: str | None = None) -> str:
    result = subprocess.run(
        ["kubectl", *args], input=input_text, text=True,
        capture_output=True, check=True,
    )
    return result.stdout


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def cluster_objects(kind: str) -> list[dict]:
    return json.loads(kubectl("get", kind, "-n", NAMESPACE, "-o", "json")).get("items", [])


def resource_name(kind: str, fragment: str) -> str:
    items = cluster_objects(kind)
    names = [item.get("metadata", {}).get("name", "") for item in items]
    matches = [name for name in names if fragment in name]
    if not matches:
        raise RuntimeError(f"no {kind} containing {fragment!r} found in namespace {NAMESPACE}")
    return matches[0]


def influx_pod() -> str:
    if INFLUX_POD:
        return INFLUX_POD
    pods = cluster_objects("pods")
    candidates = [
        pod for pod in pods
        if "influxdb" in pod.get("metadata", {}).get("name", "").lower()
        and pod.get("status", {}).get("phase") == "Running"
    ]
    if not candidates:
        raise RuntimeError(f"no running InfluxDB pod found in namespace {NAMESPACE}")
    return candidates[0]["metadata"]["name"]


def token_secret() -> tuple[str, str]:
    secrets = cluster_objects("secrets")
    if TOKEN_SECRET:
        candidates = [item for item in secrets if item.get("metadata", {}).get("name") == TOKEN_SECRET]
    else:
        candidates = [
            item for item in secrets
            if "influxdb" in item.get("metadata", {}).get("name", "").lower()
            and TOKEN_KEY in item.get("data", {})
        ]
    if not candidates:
        detail = TOKEN_SECRET or f"an InfluxDB secret containing {TOKEN_KEY}"
        raise RuntimeError(f"cannot find {detail} in namespace {NAMESPACE}")
    item = candidates[0]
    value = item.get("data", {}).get(TOKEN_KEY)
    if not value:
        raise RuntimeError(f"secret {item['metadata']['name']} has no {TOKEN_KEY} key")
    return item["metadata"]["name"], base64.b64decode(value).decode().strip()


def alumet_clients() -> list[dict]:
    return [
        pod for pod in cluster_objects("pods")
        if "alumet-relay-client" in pod.get("metadata", {}).get("name", "")
    ]


def is_ready(pod: dict) -> bool:
    conditions = pod.get("status", {}).get("conditions", [])
    return any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)


def preflight() -> tuple[str, str, int]:
    clients = alumet_clients()
    ready = [pod for pod in clients if is_ready(pod)]
    if not clients:
        raise RuntimeError(f"no Alumet relay client pods found in namespace {NAMESPACE}")
    if len(ready) != len(clients):
        raise RuntimeError(f"only {len(ready)}/{len(clients)} Alumet relay clients are Ready")
    pod = influx_pod()
    secret, _ = token_secret()
    kubectl("exec", "-n", NAMESPACE, pod, "--", "influx", "ping")
    recent = run_query(
        f"from(bucket: {json.dumps(BUCKET)}) |> range(start: -2m) "
        '|> filter(fn: (r) => r._field == "value" and r._measurement =~ /energy/) '
        "|> limit(n: 1)",
        pod,
    )
    if not any(True for _ in influx_rows(recent)):
        raise RuntimeError("InfluxDB has no Alumet energy measurement from the last 2 minutes")
    return pod, secret, len(ready)


def start(run_dir: Path) -> None:
    pod, secret, clients = preflight()
    energy_dir = run_dir / "energy"
    energy_dir.mkdir(parents=True, exist_ok=True)
    window = {
        "provider": "alumet",
        "started_at": utc_now(),
        "namespace": NAMESPACE,
        "influx_pod": pod,
        "token_secret": secret,
        "organization": ORG,
        "bucket": BUCKET,
        "ready_clients": clients,
    }
    (energy_dir / "alumet-window.json").write_text(json.dumps(window, indent=2), encoding="utf-8")
    print(f"Alumet ready ({clients} client pod(s)); results dir: {run_dir}")


def run_query(flux: str, pod: str | None = None) -> str:
    pod = pod or influx_pod()
    _, token = token_secret()
    # Feed the token on stdin so it is not exposed in the local or remote process list.
    script = 'IFS= read -r INFLUX_TOKEN; export INFLUX_TOKEN; exec influx query --raw --org "$1" "$2"'
    return kubectl(
        "exec", "-i", "-n", NAMESPACE, pod, "--", "sh", "-c", script,
        "alumet-query", ORG, flux, input_text=f"{token}\n",
    )


def run_influx(*args: str) -> str:
    pod = influx_pod()
    _, token = token_secret()
    script = 'IFS= read -r INFLUX_TOKEN; export INFLUX_TOKEN; exec influx "$@"'
    return kubectl(
        "exec", "-i", "-n", NAMESPACE, pod, "--", "sh", "-c", script,
        "alumet-influx", *args, input_text=f"{token}\n",
    )


def set_retention(duration: str) -> None:
    buckets = json.loads(run_influx("bucket", "list", "--org", ORG, "--json"))
    bucket = next((item for item in buckets if item.get("name") == BUCKET), None)
    if not bucket:
        raise RuntimeError(f"InfluxDB bucket not found: {BUCKET}")
    run_influx(
        "bucket", "update", "--id", bucket["id"], "--org", ORG,
        "--retention", duration,
    )
    print(f"InfluxDB bucket {BUCKET}: retention={duration}")


def eligible_nodes() -> list[str]:
    nodes = json.loads(kubectl("get", "nodes", "-o", "json")).get("items", [])
    result = []
    for node in nodes:
        spec = node.get("spec", {})
        disabled = any(
            taint.get("key") == "eaer.alumet/disabled"
            and taint.get("effect") in {"NoSchedule", "NoExecute"}
            for taint in spec.get("taints", [])
        )
        if not spec.get("unschedulable") and not disabled:
            result.append(node["metadata"]["name"])
    return result


def enable_collection() -> None:
    daemonset = resource_name("daemonsets", "alumet-relay-client")
    server = resource_name("deployments", "alumet-relay-server")
    influx = resource_name("statefulsets", "influxdb")
    selector = json.dumps({"spec": {"template": {"spec": {"nodeSelector": {ENABLE_LABEL: "true"}}}}})
    kubectl("patch", "daemonset", daemonset, "-n", NAMESPACE, "--type=merge", "-p", selector)
    kubectl("scale", "statefulset", influx, "-n", NAMESPACE, "--replicas=1")
    kubectl("rollout", "status", f"statefulset/{influx}", "-n", NAMESPACE, "--timeout=5m")
    kubectl("scale", "deployment", server, "-n", NAMESPACE, "--replicas=1")
    kubectl("rollout", "status", f"deployment/{server}", "-n", NAMESPACE, "--timeout=5m")
    nodes = eligible_nodes()
    if not nodes:
        raise RuntimeError("no schedulable Alumet node is enabled")
    for node in nodes:
        kubectl("label", "node", node, f"{ENABLE_LABEL}=true", "--overwrite")
    kubectl("rollout", "status", f"daemonset/{daemonset}", "-n", NAMESPACE, "--timeout=5m")
    set_retention(RETENTION)
    print(f"Alumet collection started on {len(nodes)} node(s)")


def disable_collection() -> None:
    daemonset = resource_name("daemonsets", "alumet-relay-client")
    server = resource_name("deployments", "alumet-relay-server")
    selector = json.dumps({"spec": {"template": {"spec": {"nodeSelector": {ENABLE_LABEL: "true"}}}}})
    kubectl("patch", "daemonset", daemonset, "-n", NAMESPACE, "--type=merge", "-p", selector)
    nodes = json.loads(kubectl("get", "nodes", "-o", "json")).get("items", [])
    for node in nodes:
        if ENABLE_LABEL in node.get("metadata", {}).get("labels", {}):
            kubectl("label", "node", node["metadata"]["name"], f"{ENABLE_LABEL}-")
    kubectl("scale", "deployment", server, "-n", NAMESPACE, "--replicas=0")
    print("Alumet collection stopped; InfluxDB and its PVC remain running")


def storage_location() -> str:
    pvcs = cluster_objects("pvc")
    pvc = next((item for item in pvcs if "influxdb" in item.get("metadata", {}).get("name", "")), None)
    if not pvc:
        return "InfluxDB PVC: not found"
    spec = pvc.get("spec", {})
    volume = spec.get("volumeName", "")
    location = "unbound"
    if volume:
        pv = json.loads(kubectl("get", "pv", volume, "-o", "json"))
        source = pv.get("spec", {})
        if source.get("nfs"):
            location = f"nfs://{source['nfs'].get('server')}{source['nfs'].get('path')}"
        elif source.get("local"):
            location = source["local"].get("path", "local volume")
        elif source.get("hostPath"):
            location = source["hostPath"].get("path", "hostPath")
        elif source.get("csi"):
            location = f"csi:{source['csi'].get('driver')}:{source['csi'].get('volumeHandle')}"
    return (
        f"InfluxDB PVC: {pvc['metadata']['name']} status={pvc.get('status', {}).get('phase', '')} "
        f"class={spec.get('storageClassName', '')} volume={volume}\n"
        f"Pod mount: /var/lib/influxdb2\nPhysical backing: {location}"
    )


def status() -> None:
    print(storage_location())
    print(kubectl("get", "pods", "-n", NAMESPACE, "-o", "wide").rstrip())
    try:
        print(run_influx("bucket", "list", "--org", ORG).rstrip())
    except (RuntimeError, subprocess.CalledProcessError):
        print("InfluxDB bucket status: unavailable", file=sys.stderr)


def query_window(window: dict) -> str:
    stop = window["ended_at"]
    flux = (
        f"from(bucket: {json.dumps(BUCKET)}) "
        f"|> range(start: time(v: {json.dumps(window['started_at'])}), "
        f"stop: time(v: {json.dumps(stop)}))"
    )
    return run_query(flux)


def influx_rows(text: str):
    header: list[str] | None = None
    for row in csv.reader(text.splitlines()):
        if not row or row[0].startswith("#"):
            continue
        if "_time" in row and "_value" in row:
            header = row
            continue
        if header and len(row) == len(header):
            yield dict(zip(header, row))


def joule_factor(metric: str) -> float:
    name = metric.lower()
    if name.endswith("_kj"):
        return 1_000.0
    if name.endswith("_mj"):
        return 1e-3
    if name.endswith("_uj"):
        return 1e-6
    if name.endswith("_nj"):
        return 1e-9
    return 1.0


def rapl_total(domains: dict[str, float]) -> float:
    """Choose non-overlapping RAPL domains instead of summing every sub-domain."""
    platform = domains.get("platform_total", domains.get("platform", 0.0))
    if platform:
        return platform
    package = domains.get("package_total", domains.get("package", 0.0))
    dram = domains.get("dram_total", domains.get("dram", 0.0))
    if package or dram:
        return package + dram
    return sum(domains.values())


def workload_pods(run_dir: Path) -> set[str]:
    path = run_dir / "placement.tsv"
    if not path.exists():
        return set()
    with path.open(encoding="utf-8") as stream:
        return {row.get("pod", "") for row in csv.DictReader(stream, delimiter="\t") if row.get("pod")}


def summarize(run_dir: Path, raw: str) -> bool:
    by_metric: dict[str, float] = defaultdict(float)
    by_node: dict[str, float] = defaultdict(float)
    by_consumer: dict[str, float] = defaultdict(float)
    by_workload_pod: dict[str, float] = defaultdict(float)
    by_system_consumer: dict[str, float] = defaultdict(float)
    rapl_by_node: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    eaer_pods = workload_pods(run_dir)
    hardware_total = 0.0
    attributed_total = 0.0
    workload_total = 0.0
    system_total = 0.0
    unknown_total = 0.0
    points = 0

    for row in influx_rows(raw):
        metric = row.get("_measurement", "")
        field = row.get("_field", "")
        if "energy" not in metric.lower() or field not in {"", "value"}:
            continue
        try:
            value = float(row.get("_value", "")) * joule_factor(metric)
        except ValueError:
            continue
        if not math.isfinite(value):
            continue
        points += 1
        by_metric[metric] += value
        consumer_kind = row.get("resource_consumer_kind") or row.get("consumer_kind", "")
        attributed = "attributed" in metric.lower() or consumer_kind not in {"", "local_machine"}
        if attributed:
            attributed_total += value
            task = next((row.get(key, "") for key in ("name", "pod", "pod_name", "k8s_pod_name") if row.get(key)), "unknown")
            by_consumer[task] += value
            if task in eaer_pods:
                workload_total += value
                by_workload_pod[task] += value
            elif task == "unknown":
                unknown_total += value
            else:
                system_total += value
                by_system_consumer[task] += value
        else:
            node = row.get("node") or row.get("node_name") or "unknown"
            if metric.lower().startswith("rapl_"):
                rapl_by_node[node][row.get("domain", "unknown").lower()] += value
            else:
                hardware_total += value
                by_node[node] += value

    for node, domains in rapl_by_node.items():
        value = rapl_total(domains)
        hardware_total += value
        by_node[node] += value

    summary = {
        "provider": "alumet",
        "measurement_status": "complete" if points else "failed",
        "total_energy_j": round(hardware_total if hardware_total else attributed_total, 6),
        "hardware_energy_j": round(hardware_total, 6),
        "attributed_energy_j": round(attributed_total, 6),
        "workload_attributed_energy_j": round(workload_total, 6),
        "system_attributed_energy_j": round(system_total, 6),
        "unknown_attributed_energy_j": round(unknown_total, 6),
        "energy_point_count": points,
        "by_task_j": {key: round(value, 6) for key, value in sorted(by_workload_pod.items())},
        "by_workload_pod_j": {key: round(value, 6) for key, value in sorted(by_workload_pod.items())},
        "by_system_consumer_j": {
            key: round(value, 6) for key, value in sorted(by_system_consumer.items())
        },
        "by_consumer_j": {key: round(value, 6) for key, value in sorted(by_consumer.items())},
        "by_node_j": {key: round(value, 6) for key, value in sorted(by_node.items())},
        "by_metric_j": {key: round(value, 6) for key, value in sorted(by_metric.items())},
        "rapl_domains_j": {
            node: {domain: round(value, 6) for domain, value in sorted(domains.items())}
            for node, domains in sorted(rapl_by_node.items())
        },
        "note": (
            "Hardware and attributed energy are reported separately. RAPL total prefers "
            "platform, otherwise package+dram, to avoid adding overlapping sub-domains. "
            "Workload consumers are matched exactly against placement.tsv; unmatched named "
            "consumers are system, and consumers without a Pod name are unknown."
        ),
    }
    path = run_dir / "energy" / "summary.json"
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return points > 0


def stop(run_dir: Path) -> None:
    path = run_dir / "energy" / "alumet-window.json"
    if not path.exists():
        raise RuntimeError(f"Alumet window not found: {path}")
    window = json.loads(path.read_text(encoding="utf-8"))
    window["ended_at"] = utc_now()
    path.write_text(json.dumps(window, indent=2), encoding="utf-8")
    # Let the relay client flush samples that already belong to the recorded window.
    time.sleep(DRAIN_SECONDS)
    raw = query_window(window)
    (run_dir / "energy" / "alumet-raw.csv").write_text(raw, encoding="utf-8")
    if not summarize(run_dir, raw):
        raise RuntimeError("Alumet returned no energy measurements for this run window")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", choices=("preflight", "start", "stop", "status", "retention", "summarize")
    )
    parser.add_argument("run_dir", nargs="?", type=Path)
    parser.add_argument("--duration", default=RETENTION)
    args = parser.parse_args()
    try:
        if args.command == "preflight":
            pod, secret, clients = preflight()
            print(f"ready_clients={clients} influx_pod={pod} token_secret={secret}")
        elif args.command == "start":
            if args.run_dir:
                start(args.run_dir)
            else:
                enable_collection()
        elif args.command == "stop" and not args.run_dir:
            disable_collection()
        elif args.command == "status":
            status()
        elif args.command == "retention":
            set_retention(args.duration)
        elif args.command == "summarize":
            if not args.run_dir:
                parser.error("run_dir is required for summarize")
            raw_path = args.run_dir / "energy" / "alumet-raw.csv"
            if not raw_path.exists():
                raise RuntimeError(f"Alumet raw export not found: {raw_path}")
            if not summarize(args.run_dir, raw_path.read_text(encoding="utf-8")):
                raise RuntimeError("Alumet raw export contains no energy measurements")
        else:
            if not args.run_dir:
                parser.error("run_dir is required for stop")
            stop(args.run_dir)
    except (RuntimeError, subprocess.CalledProcessError) as error:
        if isinstance(error, subprocess.CalledProcessError) and error.stderr:
            print(error.stderr.strip(), file=sys.stderr)
        message = error if isinstance(error, RuntimeError) else f"command failed with exit code {error.returncode}"
        raise SystemExit(f"Alumet error: {message}") from None


if __name__ == "__main__":
    main()
