#!/usr/bin/env python3
"""Small, dependency-free placement policy engine used by compiler/runtime tools."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


STRATEGIES = {"B0", *(f"C{i}" for i in range(1, 9)), "H1", "H2"}
LEVEL = {"none": 0, "low": 1, "medium": 2, "high": 3}


@dataclass(frozen=True)
class Placement:
    node: str
    score: float
    eligible: bool = True
    reason: str = ""


def _level(value: Any, default: int = 1) -> int:
    if isinstance(value, (int, float)):
        return max(0, min(3, int(value)))
    return LEVEL.get(str(value or "").lower(), default)


def _ratio(value: Any, default: float = 0.5) -> float:
    try:
        number = float(str(value).strip().removesuffix("%"))
    except (TypeError, ValueError):
        return default
    if number > 1:
        number /= 100
    return max(0.0, min(1.0, number))


def _gpu(node: dict[str, Any]) -> bool:
    return str(node.get("gpu_type", "none")).lower() != "none" or bool(node.get("gpu"))


def _data_objects(task: dict[str, Any], data: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    ids = task.get("data_ids", [])
    if isinstance(ids, str):
        ids = [ids]
    return [data[item] for item in ids if item in data]


def _values(value: Any) -> list[Any]:
    return value if isinstance(value, list) else ([] if value is None else [value])


def _size_weight(value: Any) -> float:
    return {"kb": 0.1, "mb": 0.3, "gb": 0.7, "tb": 1.0}.get(str(value).lower(), 0.5)


def _energy(value: Any) -> float:
    """Convert low/medium/high or a positive measurement to an efficiency score."""
    if isinstance(value, (int, float)):
        return 1.0 / (1.0 + max(0.0, float(value)))
    return {"low": 1.0, "medium": 0.5, "high": 0.0}.get(str(value).lower(), 0.5)


def _resource_score(task: dict[str, Any], node: dict[str, Any]) -> tuple[float, bool, str]:
    pairs = (("cpu_request", "cpu_class"), ("memory_request", "memory_class"))
    scores: list[float] = []
    for request_key, capacity_key in pairs:
        requested = _level(task.get(request_key), 1)
        capacity = _level(node.get(capacity_key), 2)
        if capacity < requested:
            return 0.0, False, f"{capacity_key} below request"
        scores.append(1.0 - abs(capacity - requested) / 3.0)
    if task.get("gpu_required") and not _gpu(node):
        return 0.0, False, "GPU required"
    if str(task.get("io_intensity", "low")).lower() == "high":
        scores.append(_level(node.get("io_class"), 1) / 3.0)
    return sum(scores) / len(scores), True, "resource fit"


def _score_one(
    strategy: str,
    task: dict[str, Any],
    node_name: str,
    node: dict[str, Any],
    data: dict[str, dict[str, Any]],
    preferences: dict[str, Any],
) -> tuple[float, bool, str]:
    if node.get("schedulable") is False:
        return 0.0, False, "node disabled"
    if strategy == "B0":
        return 0.0, True, "Kubernetes default"
    if strategy == "C1":
        if task.get("gpu_required"):
            return (1.0, True, "GPU match") if _gpu(node) else (0.0, False, "GPU required")
        allow_gpu = bool(preferences.get("allow_gpu_for_cpu", False))
        return ((0.25 if _gpu(node) else 1.0), allow_gpu or not _gpu(node), "CPU/GPU separation")
    if strategy == "C2":
        wanted = str(task.get("workload_mode", "any")).lower()
        supported = node.get("workload_modes", node.get("workload_mode", "any"))
        if isinstance(supported, str):
            supported = [supported]
        supported = [str(value).lower() for value in supported]
        matched = wanted in supported or "any" in supported
        return (1.0 if matched else 0.0, matched, "mode fit" if matched else "mode mismatch")
    if strategy == "C3":
        return _resource_score(task, node)
    if strategy == "C4":
        objects = _data_objects(task, data)
        if not objects:
            return 0.5, True, "no data location declared"
        total = sum(_size_weight(obj.get("size_class")) for obj in objects)
        local = sum(
            _size_weight(obj.get("size_class"))
            for obj in objects
            if node_name in _values(obj.get("locations"))
        )
        return (local / total if total else 0.5), True, "data locality"
    if strategy == "C5":
        objects = _data_objects(task, data)
        if not objects:
            return 0.5, True, "no storage declared"
        access = {str(value).lower() for value in _values(node.get("storage_access"))}
        matches = [str(obj.get("storage_class", "")).lower() in access for obj in objects]
        if not any(matches):
            return 0.0, False, "storage inaccessible"
        io = _level(node.get("io_class"), 1) / 3.0
        return (0.7 * sum(matches) / len(matches) + 0.3 * io), True, "storage fit"
    if strategy == "C6":
        deadline = str(task.get("deadline_class", "none")).lower()
        if deadline in {"none", "flexible"}:
            return 0.5, True, "no strict deadline"
        cpu = _level(node.get("cpu_class"), 2) / 3.0
        memory = _level(node.get("memory_class"), 2) / 3.0
        io = _level(node.get("io_class"), 1) / 3.0
        return (0.5 * cpu + 0.25 * memory + 0.25 * io), True, "deadline performance"
    if strategy == "C7":
        profile = node.get("historical_energy", 0.5)
        if isinstance(profile, dict):
            profile = profile.get(task.get("task_id"), profile.get("default", 0.5))
        return _energy(profile), True, "historical energy"
    if strategy == "C8":
        return _energy(node.get("carbon_intensity")), True, "grid carbon intensity"
    if strategy == "H1":
        power = node.get("power_watts", node.get("metrics", {}).get("power_watts"))
        carbon = _energy(node.get("carbon_intensity", "medium"))
        max_power = max(1.0, float(preferences.get("max_power_watts", 500)))
        power_score = 1.0 - min(max(0.0, float(power)) / max_power, 1.0) if power is not None else 0.5
        return 0.75 * power_score + 0.25 * carbon, True, "runtime energy"
    if strategy == "H2":
        metrics = node.get("metrics", {})
        cpu = _ratio(node.get("cpu_utilization", metrics.get("cpu_utilization")))
        memory = _ratio(node.get("memory_utilization", metrics.get("memory_utilization")))
        return 1.0 - (cpu + memory) / 2.0, True, "runtime load"
    raise ValueError(f"Unknown strategy '{strategy}'")


def _strategies(value: Any) -> list[str]:
    values: Iterable[Any] = value if isinstance(value, list) else [value or "B0"]
    result = [str(item).upper() for item in values]
    unknown = set(result) - STRATEGIES
    if unknown:
        raise ValueError(f"Unknown strategies: {', '.join(sorted(unknown))}")
    return result


def rank_nodes(
    task: dict[str, Any],
    nodes: dict[str, dict[str, Any]],
    data: dict[str, dict[str, Any]] | None = None,
    strategy: str | list[str] = "B0",
    preferences: dict[str, Any] | None = None,
) -> list[Placement]:
    """Return deterministic best-first placement candidates.

    A list of strategies is a simple equal-weight composition. Ineligible in any selected
    strategy means ineligible overall; numeric weights can be supplied in
    preferences.weights, e.g. {C3: 2, C7: 1}.
    """
    data = data or {}
    preferences = preferences or {}
    methods = _strategies(strategy)
    weights = preferences.get("weights", {})
    placements: list[Placement] = []
    for node_name, node in nodes.items():
        total = 0.0
        total_weight = 0.0
        eligible = True
        reasons: list[str] = []
        for method in methods:
            score, method_eligible, reason = _score_one(
                method, task, node_name, node or {}, data, preferences
            )
            weight = float(weights.get(method, 1.0))
            total += score * weight
            total_weight += weight
            eligible = eligible and method_eligible
            reasons.append(f"{method}: {reason}")
        placements.append(Placement(
            node=node_name,
            score=round(total / total_weight if total_weight else 0.0, 6),
            eligible=eligible,
            reason="; ".join(reasons),
        ))
    return sorted(placements, key=lambda item: (not item.eligible, -item.score, item.node))


def _objects_by_id(raw: Any, id_key: str) -> dict[str, dict[str, Any]]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(key): (value or {}) for key, value in raw.items()}
    if isinstance(raw, list):
        return {
            str(item[id_key]): {key: value for key, value in item.items() if key != id_key}
            for item in raw
        }
    raise ValueError(f"'{id_key.removesuffix('_id')}s' must be a mapping or list")


def compile_policy_config(config: dict[str, Any]) -> dict[str, dict[str, dict[str, list[str]]]]:
    """Compile version-2 preference config into compiler.py's small affinity rule form."""
    nodes = _objects_by_id(config.get("nodes"), "node_id")
    data = _objects_by_id(config.get("data"), "data_id")
    global_strategy = config.get("strategy", "B0")
    global_preferences = config.get("preferences", {}) or {}
    output: dict[str, dict[str, dict[str, list[str]]]] = {}

    for group_name in ("batch", "incremental"):
        group = config.get(group_name)
        if not group:
            continue
        defaults = group.get("defaults", {}) or {}
        templates = group.get("templates", {}) or {}
        if not isinstance(templates, dict):
            raise ValueError(f"{group_name}.templates must be a mapping")
        rules: dict[str, dict[str, list[str]]] = {}
        for task_name, raw in templates.items():
            raw = raw or {}
            task = dict(defaults.get("task", {}) or {})
            task.update(raw.get("task", {}) or {})
            task["task_id"] = str(task_name)
            method = raw.get("strategy", defaults.get("strategy", global_strategy))
            preferences = dict(global_preferences)
            preferences.update(defaults.get("preferences", {}) or {})
            preferences.update(raw.get("preferences", {}) or {})
            methods = _strategies(method)

            if methods == ["B0"]:
                # B0 leaves ranking to Kubernetes, but still honors the configured node
                # availability switch. Requiring all enabled nodes gives the default
                # scheduler full freedom within that set while hard-blocking nodes with
                # schedulable: false.
                enabled_nodes = [
                    name for name, node in nodes.items()
                    if (node or {}).get("schedulable") is not False
                ]
                if nodes and not enabled_nodes:
                    raise ValueError(f"No schedulable node for {group_name}.{task_name}")
                rule = {
                    "require": enabled_nodes,
                    "tags": ["gpu"] if task.get("gpu_required") else [],
                    "prefer": [],
                    "fallback": [],
                    "avoid": [],
                }
            else:
                if not nodes:
                    raise ValueError("At least one node is required for strategies other than B0")
                ranking = rank_nodes(task, nodes, data, methods, preferences)
                eligible = [item for item in ranking if item.eligible]
                if not eligible:
                    details = "; ".join(f"{item.node} ({item.reason})" for item in ranking)
                    raise ValueError(f"No eligible node for {group_name}.{task_name}: {details}")
                best = eligible[0].score
                prefer = [item.node for item in eligible if item.score == best]
                fallback = [item.node for item in eligible if item.score != best]
                rule = {
                    "require": [item.node for item in eligible],
                    "tags": ["gpu"] if task.get("gpu_required") else [],
                    "prefer": prefer,
                    "fallback": fallback,
                    "avoid": [item.node for item in ranking if not item.eligible],
                }

            # Explicit lists are useful for experiments and override computed ordering.
            for key in ("require", "tags", "prefer", "fallback", "avoid"):
                if key in raw:
                    value = raw[key]
                    rule[key] = [str(value)] if isinstance(value, str) else list(value or [])
            rules[str(task_name).strip().lower().replace("_", "-")] = rule
        output[group_name] = rules
    return output
