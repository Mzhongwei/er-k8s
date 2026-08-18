#!/usr/bin/env python3
"""Resolve external carbon and historical-energy inputs for scheduling policies."""
from __future__ import annotations

import copy
import ipaddress
import json
import math
import os
import re
import shlex
import subprocess
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


RTE_SOURCE = "https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr/api/"
RTE_API = (
    "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "eco2mix-national-tr/records?select=taux_co2,date_heure&where="
    "taux_co2%20is%20not%20null&order_by=date_heure%20desc&limit=1"
)
ELECTRICITY_MAPS_SOURCE = "https://app.electricitymaps.com/developer-hub/api/reference"
ELECTRICITY_MAPS_API = "https://api.electricitymaps.com/v4/carbon-intensity/latest?zone={zone}"
IP_GEOLOCATION_SOURCE = "https://ipapi.co/api/"
IP_GEOLOCATION_API = "https://ipapi.co/{ip}/json/"
UNCONFIRMED_ZONE = "NEEDS_CONFIRMATION"
INCLUDE_KEYS = {
    "nodes": {"nodes"},
    "workloads": {"batch", "incremental"},
    "data": {"data"},
}


def _cpu_millicores(value: Any) -> int:
    text = str(value or "0").strip()
    factors = {"n": 0.000001, "u": 0.001, "m": 1.0}
    suffix = text[-1:] if text[-1:] in factors else ""
    number = float(text[:-1] if suffix else text)
    return round(number * factors.get(suffix, 1000.0))


def _memory_bytes(value: Any) -> int:
    match = re.fullmatch(
        r"([0-9]+(?:\.[0-9]+)?)([KMGTPE]i?|)", str(value or "0")
    )
    if not match:
        raise ValueError(f"Unsupported Kubernetes memory quantity: {value}")
    number, suffix = match.groups()
    powers = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}
    base = 1024 if suffix.endswith("i") else 1000
    return round(float(number) * base ** powers[suffix.removesuffix("i")])


def load_policy_config(path: Path) -> dict[str, Any]:
    """Load one scheduling entry point and its strictly scoped YAML fragments."""
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    main = _read_yaml(path)
    allowed_main = {"version", "strategy", "preferences", "includes"}
    unknown_main = set(main) - allowed_main
    if unknown_main:
        raise ValueError(
            f"Unexpected key(s) in {path}: {', '.join(sorted(unknown_main))}; "
            "put nodes, workloads, and data in their included files"
        )
    if main.get("version") != 2:
        raise ValueError("scheduling configuration requires version: 2")
    includes = main.get("includes")
    if not isinstance(includes, dict):
        raise ValueError(f"{path} requires an includes mapping")
    missing = set(INCLUDE_KEYS) - set(includes)
    unknown = set(includes) - set(INCLUDE_KEYS)
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing {', '.join(sorted(missing))}")
        if unknown:
            details.append(f"unknown {', '.join(sorted(unknown))}")
        raise ValueError(f"Invalid includes in {path}: {'; '.join(details)}")

    merged = {key: value for key, value in main.items() if key != "includes"}
    for include_name, allowed_keys in INCLUDE_KEYS.items():
        raw_path = includes[include_name]
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise ValueError(f"includes.{include_name} must be a YAML file path")
        include_path = Path(raw_path)
        if not include_path.is_absolute():
            include_path = path.parent / include_path
        include_path = include_path.resolve()
        if not include_path.exists():
            raise FileNotFoundError(
                f"Included config file not found: {include_path} (includes.{include_name})"
            )
        fragment = _read_yaml(include_path)
        unexpected = set(fragment) - allowed_keys
        if unexpected:
            raise ValueError(
                f"Unexpected key(s) in {include_path}: {', '.join(sorted(unexpected))}; "
                f"allowed: {', '.join(sorted(allowed_keys))}"
            )
        # Require at least one allowed key, not all of them: a workloads fragment may define
        # only `batch` or only `incremental` (the downstream compile skips an absent group).
        if not set(fragment) & allowed_keys:
            raise ValueError(
                f"{include_path} (includes.{include_name}) must define at least one of: "
                f"{', '.join(sorted(allowed_keys))}"
            )
        merged.update(fragment)
    return merged


def _strategy_names(value: Any) -> set[str]:
    if isinstance(value, dict):
        names: set[str] = set()
        for key, child in value.items():
            if key == "strategy":
                values = child if isinstance(child, list) else [child]
                names.update(str(item).upper() for item in values)
            else:
                names.update(_strategy_names(child))
        return names
    if isinstance(value, list):
        return set().union(*(_strategy_names(item) for item in value)) if value else set()
    return set()


def _remove_c7(value: Any) -> None:
    if not isinstance(value, dict):
        return
    for key, child in value.items():
        if key == "strategy":
            methods = child if isinstance(child, list) else [child]
            if any(str(item).upper() == "C7" for item in methods):
                remaining = [item for item in methods if str(item).upper() != "C7"]
                value[key] = (remaining or ["B0"]) if isinstance(child, list) else "B0"
        elif isinstance(child, dict):
            _remove_c7(child)


def _nodes(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = config.get("nodes") or {}
    if isinstance(raw, dict):
        result = {}
        for name, properties in raw.items():
            if properties is None:
                properties = raw[name] = {}
            result[str(name)] = properties
        return result
    return {
        str(item["node_id"]): item
        for item in raw
        if isinstance(item, dict) and item.get("node_id")
    }


def _historical_profiles(
    results_dir: Path, node_names: set[str]
) -> tuple[dict[str, float], int]:
    """Return mean per-run relative node energy; normalization allows mixed monitors."""
    samples: dict[str, list[float]] = defaultdict(list)
    used_runs = 0
    paths = list(results_dir.glob("*/energy/*-summary.json"))
    paths.extend(results_dir.glob("*/energy/summary.json"))
    for path in sorted(paths):
        try:
            summary = json.loads(path.read_text(encoding="utf-8"))
            raw = summary.get("by_node_j") or {}
            values = {
                str(node): float(energy)
                for node, energy in raw.items()
                if str(node) in node_names
                and math.isfinite(float(energy))
                and float(energy) > 0
            }
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
        if not values:
            continue
        baseline = min(values.values())
        for node, energy in values.items():
            samples[node].append(energy / baseline)
        used_runs += 1
    return ({node: sum(items) / len(items) for node, items in samples.items()}, used_runs)


def _apply_history(config: dict[str, Any], results_dir: Path) -> bool:
    nodes = _nodes(config)
    profiles, run_count = _historical_profiles(results_dir, set(nodes))
    if not profiles:
        _remove_c7(config)
        print(
            f"Warning: C7 selected but no usable energy summaries were found under "
            f"{results_dir}; removing C7 (using B0 only when no strategy remains).",
            file=sys.stderr,
        )
        return False
    worst = max(profiles.values())
    for name, properties in nodes.items():
        properties["historical_energy"] = profiles.get(name, worst)
    values = ", ".join(
        f"{name}={properties['historical_energy']:.3f}"
        for name, properties in nodes.items()
    )
    print(f"C7 history: {run_count} run(s), relative node energy index: {values}")
    return True


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as stream:
        value = yaml.safe_load(stream) or {}
    if not isinstance(value, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return value


def _cluster_nodes() -> list[dict[str, Any]]:
    result = subprocess.run(
        ["kubectl", "get", "nodes", "-o", "json"],
        text=True, capture_output=True, check=False,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "kubectl get nodes failed")
    return json.loads(result.stdout).get("items", [])


def _apply_cluster_capabilities(
    config: dict[str, Any], cluster_nodes: list[dict[str, Any]]
) -> None:
    """Merge Kubernetes-reported capacity and availability into configured nodes."""
    configured = _nodes(config)
    cluster = {
        str(item.get("metadata", {}).get("name")): item
        for item in cluster_nodes
        if item.get("metadata", {}).get("name")
    }
    cpu: dict[str, int] = {}
    memory: dict[str, int] = {}

    for name, properties in configured.items():
        node = cluster.get(name)
        manually_disabled = properties.get("schedulable") is False
        if node is None:
            properties.update({"schedulable": False, "kubernetes_status": "not-found"})
            continue

        allocatable = node.get("status", {}).get("allocatable", {}) or {}
        cpu_value = _cpu_millicores(allocatable.get("cpu"))
        memory_value = _memory_bytes(allocatable.get("memory"))
        gpu_count = int(float(allocatable.get("nvidia.com/gpu", 0)))
        labels = node.get("metadata", {}).get("labels", {}) or {}
        ready = any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in node.get("status", {}).get("conditions", [])
        )
        available = ready and not bool(node.get("spec", {}).get("unschedulable"))
        schedulable = available and not manually_disabled
        properties.update({
            "cpu_allocatable_m": cpu_value,
            "memory_allocatable_bytes": memory_value,
            "gpu_count": gpu_count,
            "gpu_type": labels.get(
                "nvidia.com/gpu.product", "generic" if gpu_count else "none"
            ),
            "schedulable": schedulable,
            "kubernetes_status": "ready" if available else "unavailable",
        })
        if schedulable:
            cpu[name] = cpu_value
            memory[name] = memory_value

    max_cpu = max(cpu.values(), default=0)
    max_memory = max(memory.values(), default=0)
    for name, properties in configured.items():
        if name in cpu:
            properties["cpu_capacity"] = cpu[name] / max_cpu if max_cpu else 0.0
            properties["memory_capacity"] = (
                memory[name] / max_memory if max_memory else 0.0
            )


def _addresses(node: dict[str, Any]) -> tuple[str, str]:
    values = {
        item.get("type", ""): item.get("address", "")
        for item in node.get("status", {}).get("addresses", [])
    }
    return values.get("InternalIP", ""), values.get("ExternalIP", "")


def _public_ip(*values: str) -> str:
    for value in values:
        try:
            if ipaddress.ip_address(value).is_global:
                return value
        except ValueError:
            continue
    return ""


def _ip_location(ip: str, template: str) -> dict[str, Any]:
    if not ip:
        return {}
    url = template.format(ip=ip)
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            value = json.load(response)
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    country = str(value.get("country_code") or value.get("country") or "").upper()
    return {
        "zone": country if re.fullmatch(r"[A-Z]{2}", country) else "",
        "city": value.get("city", ""),
        "latitude": value.get("latitude"),
        "longitude": value.get("longitude"),
    }


def _old_node_entry(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        return {"zone": raw.upper(), "location_source": "previous-config"}
    return dict(raw or {}) if isinstance(raw, dict) else {}


def _detected_zone(labels: dict[str, str], old: dict[str, Any]) -> tuple[str, str]:
    explicit = labels.get("eaer.carbon/zone", "")
    if explicit:
        return explicit.upper(), "node-label:eaer.carbon/zone"
    country = labels.get("topology.kubernetes.io/country", "")
    if country:
        return country.upper(), "node-label:topology.kubernetes.io/country"
    previous = str(old.get("zone", "")).upper()
    if previous and previous != UNCONFIRMED_ZONE:
        return previous, "previous-config"
    region = labels.get("topology.kubernetes.io/region", "").upper()
    if re.fullmatch(r"[A-Z]{2}", region):
        return region, "node-label:topology.kubernetes.io/region"
    return "", ""


def _default_zone_source(zone: str) -> dict[str, str]:
    if zone == "FR":
        return {"provider": "rte-eco2mix", "source_url": RTE_SOURCE, "api_url": RTE_API}
    return {
        "provider": "electricity-maps",
        "source_url": ELECTRICITY_MAPS_SOURCE,
        "api_url": ELECTRICITY_MAPS_API,
        "token_env": "ELECTRICITY_MAPS_API_TOKEN",
    }


def _ensure_zone_sources(carbon: dict[str, Any]) -> bool:
    zones = carbon.setdefault("zones", {})
    changed = False
    active_zones = {
        str(_old_node_entry(raw).get("zone", "")).upper()
        for raw in (carbon.get("nodes") or {}).values()
    } - {"", UNCONFIRMED_ZONE}
    cross_region = any(zone != "FR" for zone in active_zones)
    for zone in active_zones:
        if zone and zone != UNCONFIRMED_ZONE and zone not in zones:
            zones[zone] = _default_zone_source(zone)
            changed = True
        source = zones.get(zone, {}) or {}
        if (cross_region and source.get("provider") == "rte-eco2mix"
                and source.get("intensity_g_per_kwh") is None):
            zones[zone] = _default_zone_source("non-fr")
            changed = True
    return changed


def _sync_carbon_config(
    path: Path, cluster_nodes: list[dict[str, Any]]
) -> dict[str, Any]:
    previous = _read_yaml(path)
    old_nodes = previous.get("nodes") or {}
    geolocation_api = str(previous.get("ip_geolocation", {}).get("api_url", IP_GEOLOCATION_API))
    nodes: dict[str, dict[str, Any]] = {}
    detected_zones: set[str] = set()
    for node in cluster_nodes:
        name = str(node.get("metadata", {}).get("name", ""))
        if not name:
            continue
        labels = node.get("metadata", {}).get("labels", {}) or {}
        old = _old_node_entry(old_nodes.get(name))
        internal_ip, external_ip = _addresses(node)
        zone, source = _detected_zone(labels, old)
        # Only hit the geolocation endpoint when the zone is still unknown; when labels or
        # existing config already resolved it, this per-node HTTP round-trip is pure waste.
        if not zone:
            location = _ip_location(_public_ip(external_ip, internal_ip), geolocation_api)
            if location.get("zone"):
                zone, source = str(location["zone"]), "public-ip-geolocation"
        zone = zone or UNCONFIRMED_ZONE
        nodes[name] = {
            "internal_ip": internal_ip,
            "external_ip": external_ip,
            "zone": zone,
            "location_source": source or "manual-confirmation-required",
        }
        for key in ("city", "latitude", "longitude"):
            if location.get(key) not in (None, ""):
                nodes[name][key] = location[key]
        if zone != UNCONFIRMED_ZONE:
            detected_zones.add(zone)

    zones = dict(previous.get("zones") or {})
    for zone in sorted(detected_zones):
        zones.setdefault(zone, _default_zone_source(zone))
    generated = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "note": "IP/label locations are suggestions; confirm the physical electricity-grid zone.",
        "ip_geolocation": {
            "source_url": previous.get("ip_geolocation", {}).get(
                "source_url", IP_GEOLOCATION_SOURCE
            ),
            "api_url": geolocation_api,
        },
        "nodes": nodes,
        "zones": zones,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(generated, sort_keys=False), encoding="utf-8")
    return generated


def _confirm_carbon(path: Path) -> dict[str, Any]:
    if not sys.stdin.isatty():
        raise ValueError("C8 requires an interactive yes/no confirmation of node locations")
    while True:
        carbon = _read_yaml(path)
        if _ensure_zone_sources(carbon):
            path.write_text(yaml.safe_dump(carbon, sort_keys=False), encoding="utf-8")
        print("C8 Carbon-Aware Placement node locations:")
        print(f"Configuration: {path}")
        print(yaml.safe_dump(carbon, sort_keys=False).rstrip())
        answer = input("Are these node electricity-grid locations correct? [yes/no]: ").strip().lower()
        if answer in {"yes", "y"}:
            unresolved = [
                name for name, raw in (carbon.get("nodes") or {}).items()
                if str(_old_node_entry(raw).get("zone", "")).upper() in {"", UNCONFIRMED_ZONE}
            ]
            if unresolved:
                print(f"Unconfirmed node locations: {', '.join(unresolved)}", file=sys.stderr)
                editor = shlex.split(os.environ.get("EDITOR", "vi"))
                subprocess.run([*editor, str(path)], check=True)
                continue
            return carbon
        if answer in {"no", "n"}:
            editor = shlex.split(os.environ.get("EDITOR", "vi"))
            subprocess.run([*editor, str(path)], check=True)
            continue
        print("Please enter yes or no.", file=sys.stderr)


def _rte_intensity(source: dict[str, Any]) -> tuple[float, str]:
    url = str(source.get("api_url", ""))
    if not url:
        raise ValueError("RTE carbon configuration requires api_url")
    with urllib.request.urlopen(url, timeout=15) as response:
        payload = json.load(response)
    records = payload.get("results") or []
    if not records or records[0].get("taux_co2") is None:
        raise RuntimeError("RTE returned no current carbon-intensity value")
    return float(records[0]["taux_co2"]), str(records[0].get("date_heure", ""))


def _electricity_maps_intensity(zone: str, source: dict[str, Any]) -> tuple[float, str]:
    token_env = str(source.get("token_env", "ELECTRICITY_MAPS_API_TOKEN"))
    token = os.environ.get(token_env, "")
    if not token:
        raise ValueError(f"Electricity Maps zone {zone} requires environment variable {token_env}")
    url = str(source.get("api_url", ELECTRICITY_MAPS_API)).format(zone=zone)
    request = urllib.request.Request(url, headers={"auth-token": token})
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = json.load(response)
    data = payload.get("data") or []
    point = data[0] if isinstance(data, list) and data else payload
    raw = point.get("value", point.get("carbonIntensity"))
    if raw is None:
        raise RuntimeError(f"Electricity Maps returned no carbon intensity for zone {zone}")
    return float(raw), str(point.get("datetime", payload.get("datetime", "")))


def _zone_intensity(zone: str, source: dict[str, Any]) -> tuple[float, str]:
    if source.get("intensity_g_per_kwh") is not None:
        return float(source["intensity_g_per_kwh"]), str(source.get("observed_at", "manual"))
    provider = str(source.get("provider", "")).lower()
    if provider == "rte-eco2mix":
        return _rte_intensity(source)
    if provider == "electricity-maps":
        return _electricity_maps_intensity(zone, source)
    raise ValueError(f"Unsupported carbon provider for zone {zone}: {provider or 'missing'}")


def _apply_carbon(
    config: dict[str, Any], path: Path, cluster_nodes: list[dict[str, Any]]
) -> None:
    _sync_carbon_config(path, cluster_nodes)
    carbon = _confirm_carbon(path)
    node_zones = carbon.get("nodes") or {}
    zones = carbon.get("zones") or {}
    readings: dict[str, tuple[float, str]] = {}
    for name, properties in _nodes(config).items():
        if properties.get("schedulable") is False:
            continue
        entry = _old_node_entry(node_zones.get(name))
        zone = str(entry.get("zone", "")).upper()
        if not zone or zone == UNCONFIRMED_ZONE:
            raise ValueError(
                f"No carbon zone confirmed for node {name}; add it under nodes in {path}"
            )
        zone_config = zones.get(zone, {}) or {}
        if zone not in readings:
            readings[zone] = _zone_intensity(zone, zone_config)
        intensity, observed_at = readings[zone]
        source = zone_config.get("source_url", "")
        if not source:
            raise ValueError(f"No carbon-intensity source_url configured for zone {zone}")
        properties.update({
            "carbon_zone": zone,
            "carbon_intensity": intensity,
            "carbon_source": source,
            "carbon_observed_at": observed_at,
        })
        print(f"Carbon: {name} -> {zone}: {intensity:.3f} gCO2eq/kWh  source={source}")


def prepare_policy_config(
    config: dict[str, Any], config_path: Path, results_dir: Path
) -> dict[str, Any]:
    """Return a resolved copy without rewriting the user's scheduling configuration."""
    resolved = copy.deepcopy(config)
    cluster_nodes = _cluster_nodes()
    _apply_cluster_capabilities(resolved, cluster_nodes)
    methods = _strategy_names(resolved)
    if "C7" in methods:
        _apply_history(resolved, results_dir)
    if methods & {"H1", "C8"}:
        _apply_carbon(
            resolved, config_path.parent / "carbon-intensity.yaml", cluster_nodes
        )
    return resolved
