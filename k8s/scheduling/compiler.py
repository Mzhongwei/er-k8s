#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import csv
import os
import shutil
import sys
from pathlib import Path
from typing import Any

import yaml

from placement_config import load_policy_config, prepare_policy_config
from scheduler import compile_policy_config_with_plan, slug


FIELDS = ("require", "tags", "prefer", "fallback", "avoid")
PLAN_FIELDS = (
    "phase", "task", "strategies", "weights", "strategy_scores", "node", "score",
    "eligible", "role", "reason",
)
HOSTNAME_KEY = "kubernetes.io/hostname"

GPU_TOLERATION = {
    "key": "nvidia.com/gpu",
    "operator": "Equal",
    "value": "true",
    "effect": "NoSchedule",
}


# bert_matching.enabled adds BERT training to the embedding batch DAG and a bert-matching
# incremental Job; normalization then also writes record texts onto the BERT model PVC.
BERT_MATCHING_BATCH_TASKS = {"bert-normalization-training", "bert-training"}
BERT_MATCHING_JOB = "bert-matching"
BERT_MATCHING_JOB_FILE = "bert_matching.yaml"
BERT_MODEL_VOLUME = {
    "name": "pipeline-bert-model",
    "persistentVolumeClaim": {"claimName": "pipeline-bert-model-claim"},
}
BERT_MODEL_MOUNT = {"name": "pipeline-bert-model", "mountPath": "/app/data/bert"}
BERT_TRAINING_TEMPLATE = "berttrai-training"


class LiteralString(str):
    pass


class NoAliasSafeDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


def literal_str_representer(dumper: yaml.Dumper, data: LiteralString):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


NoAliasSafeDumper.add_representer(LiteralString, literal_str_representer)




def load_scheduling_bundle(
    path: Path, results_dir: Path, runtime: dict[str, Any] | None = None,
) -> tuple[dict[str, dict[str, dict[str, list[str]]]], list[dict[str, Any]]]:
    data = load_policy_config(path)
    apply_runtime_to_workloads(data, runtime or {})
    data = prepare_policy_config(data, path, results_dir)
    return compile_policy_config_with_plan(data)


def write_plan(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=PLAN_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def print_plan(rows: list[dict[str, Any]]) -> None:
    print("Scheduling plan:")
    tasks: dict[tuple[str, str, str, str], dict[str, list[str]]] = {}
    for row in rows:
        key = (row["phase"], row["task"], row["strategies"], row["weights"])
        roles = tasks.setdefault(key, {})
        roles.setdefault(row["role"], []).append(f"{row['node']}({row['score']})")
    for (phase, task, strategies, weights), roles in tasks.items():
        parts = [
            f"{role}={','.join(roles[role])}"
            for role in ("preferred", "allowed", "fallback", "avoid", "blocked")
            if roles.get(role)
        ]
        print(
            f"  {phase:<11} {task:<34} [{strategies} {weights}] "
            + " ".join(parts)
        )


def build_node_affinity(rule: dict[str, list[str]]) -> dict[str, Any] | None:
    preferred_terms: list[dict[str, Any]] = []

    if rule["prefer"]:
        preferred_terms.append({
            "weight": 100,
            "preference": {
                "matchExpressions": [{
                    "key": HOSTNAME_KEY,
                    "operator": "In",
                    "values": list(rule["prefer"]),
                }]
            }
        })

    if rule["fallback"]:
        preferred_terms.append({
            "weight": 50,
            "preference": {
                "matchExpressions": [{
                    "key": HOSTNAME_KEY,
                    "operator": "In",
                    "values": list(rule["fallback"]),
                }]
            }
        })

    if rule["avoid"]:
        preferred_terms.append({
            "weight": 10,
            "preference": {
                "matchExpressions": [{
                    "key": HOSTNAME_KEY,
                    "operator": "NotIn",
                    "values": list(rule["avoid"]),
                }]
            }
        })

    if not preferred_terms and not rule["require"]:
        return None

    node_affinity: dict[str, Any] = {}
    if rule["require"]:
        node_affinity["requiredDuringSchedulingIgnoredDuringExecution"] = {
            "nodeSelectorTerms": [{
                "matchExpressions": [{
                    "key": HOSTNAME_KEY,
                    "operator": "In",
                    "values": list(rule["require"]),
                }]
            }]
        }
    if preferred_terms:
        node_affinity["preferredDuringSchedulingIgnoredDuringExecution"] = preferred_terms
    return {"nodeAffinity": node_affinity}


def has_gpu_tag(rule: dict[str, list[str]]) -> bool:
    return "gpu" in [slug(tag) for tag in rule["tags"]]


def ensure_gpu_toleration(pod_spec_or_argo_template: dict[str, Any]) -> None:
    tolerations = pod_spec_or_argo_template.setdefault("tolerations", [])

    if not isinstance(tolerations, list):
        raise ValueError("Existing tolerations field must be a list")

    already_present = any(
        isinstance(item, dict)
        and item.get("key") == GPU_TOLERATION["key"]
        and item.get("operator") == GPU_TOLERATION["operator"]
        and item.get("value") == GPU_TOLERATION["value"]
        and item.get("effect") == GPU_TOLERATION["effect"]
        for item in tolerations
    )

    if not already_present:
        tolerations.append(copy.deepcopy(GPU_TOLERATION))


def ensure_gpu_resource(container: dict[str, Any]) -> None:
    resources = container.setdefault("resources", {})
    if not isinstance(resources, dict):
        raise ValueError("Existing container resources field must be a YAML object")
    limits = resources.setdefault("limits", {})
    if not isinstance(limits, dict):
        raise ValueError("Existing container resource limits must be a YAML object")
    limits.setdefault("nvidia.com/gpu", 1)


def remove_gpu_resource(container: dict[str, Any]) -> None:
    resources = container.get("resources", {})
    if not isinstance(resources, dict):
        return
    for resource_type in ("requests", "limits"):
        values = resources.get(resource_type)
        if isinstance(values, dict):
            values.pop("nvidia.com/gpu", None)


def _bert_device(section: Any, default: str = "auto") -> str:
    device = str(section.get("device", default) if isinstance(section, dict) else default).strip().lower()
    if device not in {"auto", "cpu", "cuda"}:
        raise ValueError("BERT device must be one of: auto, cpu, cuda")
    return device


def _device_requires_gpu(device: str) -> bool:
    # Preserve historical GPU scheduling for auto. Explicit cpu is the only mode that
    # must remove GPU resources and remain eligible for CPU-only nodes.
    return device != "cpu"


def apply_runtime_to_workloads(document: dict[str, Any], runtime: dict[str, Any]) -> None:
    task_devices = {
        ("batch", "bert-training"): runtime.get("bert_training_device", "auto"),
        ("incremental", BERT_MATCHING_JOB): runtime.get("bert_matching_device", "auto"),
    }
    for (phase, task_name), device in task_devices.items():
        templates = document.get(phase, {}).get("templates", {})
        task = templates.get(task_name) if isinstance(templates, dict) else None
        if not isinstance(task, dict):
            continue
        settings = task.setdefault("task", {})
        settings["gpu_required"] = _device_requires_gpu(str(device))


def load_pipeline_runtime(path: Path | None) -> dict[str, Any]:
    """Read resource-affecting settings from the exact pipeline config being run."""
    if path is None:
        return {}
    document = load_yaml_file(path)
    if not isinstance(document, dict):
        raise ValueError(f"{path} root must be a YAML object")

    walk = document.get("random_walk", {})
    embedding = document.get("embeddings_training", {})
    processes = int(walk.get("processes", 1)) if isinstance(walk, dict) else 1
    workers = int(embedding.get("workers", 1)) if isinstance(embedding, dict) else 1
    if processes < 1:
        raise ValueError("random_walk.processes must be at least 1")
    if workers < 1:
        raise ValueError("embeddings_training.workers must be at least 1")
    if isinstance(embedding, dict) and "device" in embedding:
        raise ValueError("embeddings_training.device is not supported by the Gensim implementation")
    bert_matching = document.get("bert_matching", {})
    bert_training = document.get("bert_training", {})
    matching_device = _bert_device(bert_matching)
    training_device = _bert_device(bert_training, matching_device)
    return {
        "random_walk_processes": processes,
        "embedding_workers": workers,
        "bert_matching": isinstance(bert_matching, dict) and bert_matching.get("enabled") is True,
        "bert_matching_device": matching_device,
        "bert_training_device": training_device,
    }


def bert_matching_enabled(runtime: dict[str, Any] | None) -> bool:
    return bool((runtime or {}).get("bert_matching"))


def add_bert_model_volume(pod_spec_or_template: dict[str, Any], container: dict[str, Any]) -> None:
    volumes = pod_spec_or_template.setdefault("volumes", [])
    if not any(volume.get("name") == BERT_MODEL_VOLUME["name"] for volume in volumes):
        volumes.append(copy.deepcopy(BERT_MODEL_VOLUME))
    mounts = container.setdefault("volumeMounts", [])
    if not any(mount.get("name") == BERT_MODEL_MOUNT["name"] for mount in mounts):
        mounts.append(copy.deepcopy(BERT_MODEL_MOUNT))


def apply_bert_matching_to_argo(workflow: dict[str, Any], enabled: bool) -> None:
    for template in get_argo_templates_container(workflow):
        if not isinstance(template, dict):
            continue
        name = slug(str(template.get("name", "")))
        if name == "embedding-training-dag" and not enabled:
            dag = template.get("dag", {})
            dag["tasks"] = [
                task for task in dag.get("tasks", [])
                if slug(str(task.get("name", ""))) not in BERT_MATCHING_BATCH_TASKS
            ]
        if name == "normalization" and enabled and isinstance(template.get("container"), dict):
            add_bert_model_volume(template, template["container"])


def keep_plan_row(row: dict[str, Any], pipeline_mode: str | None, bert_matching: bool) -> bool:
    task = row["task"]
    if pipeline_mode == "embedding-training-inference-evaluation":
        if row["phase"] == "incremental":
            return task != BERT_MATCHING_JOB or bert_matching
        return not task.startswith("bert-") or (bert_matching and task in BERT_MATCHING_BATCH_TASKS)
    if pipeline_mode == "bert-training-evaluation":
        return row["phase"] == "batch" and task.startswith("bert-")
    return True


def set_cpu_count(container: dict[str, Any], count: int) -> None:
    resources = container.setdefault("resources", {})
    requests = resources.setdefault("requests", {})
    limits = resources.setdefault("limits", {})
    requests["cpu"] = str(count)
    limits["cpu"] = str(count)


def apply_runtime_to_argo(workflow: dict[str, Any], runtime: dict[str, Any]) -> None:
    templates = {
        slug(str(template.get("name", ""))): template
        for template in get_argo_templates_container(workflow)
        if isinstance(template, dict)
    }
    random_walk = templates.get("random-walk")
    if random_walk and isinstance(random_walk.get("container"), dict):
        set_cpu_count(random_walk["container"], runtime["random_walk_processes"])

    embedding = templates.get("embedding-training")
    if embedding and isinstance(embedding.get("container"), dict):
        set_cpu_count(embedding["container"], runtime["embedding_workers"])

    bert_training = templates.get(BERT_TRAINING_TEMPLATE)
    if bert_training and isinstance(bert_training.get("container"), dict):
        if not _device_requires_gpu(runtime.get("bert_training_device", "auto")):
            remove_gpu_resource(bert_training["container"])


def apply_runtime_to_job(document: dict[str, Any], runtime: dict[str, Any]) -> None:
    name = slug(str(document.get("metadata", {}).get("name", "")))
    pod_spec = document.get("spec", {}).get("template", {}).get("spec", {})
    containers = pod_spec.get("containers", []) if isinstance(pod_spec, dict) else []
    if not containers or not isinstance(containers[0], dict):
        return
    if name == "random-walk":
        set_cpu_count(containers[0], runtime["random_walk_processes"])
    if name == "embedding-training":
        set_cpu_count(containers[0], runtime["embedding_workers"])


def apply_rule_to_argo_template(
    template: dict[str, Any],
    rule: dict[str, list[str]],
) -> None:
    affinity = build_node_affinity(rule)

    if affinity:
        existing_affinity = template.setdefault("affinity", {})

        if not isinstance(existing_affinity, dict):
            raise ValueError(
                f"Template '{template.get('name')}' affinity must be a YAML object"
            )

        existing_affinity["nodeAffinity"] = affinity["nodeAffinity"]

    if has_gpu_tag(rule):
        ensure_gpu_toleration(template)
        container = template.get("container")
        if isinstance(container, dict):
            ensure_gpu_resource(container)

        existing_patch = str(template.get("podSpecPatch", "") or "")

        if "runtimeClassName" not in existing_patch:
            if existing_patch.strip():
                existing_patch = existing_patch.rstrip() + "\n"

            existing_patch += "runtimeClassName: nvidia\n"

        template["podSpecPatch"] = LiteralString(existing_patch)


def apply_rule_to_kubernetes_job(
    job: dict[str, Any],
    rule: dict[str, list[str]],
) -> None:
    try:
        pod_spec = job["spec"]["template"]["spec"]
    except KeyError as error:
        raise ValueError("Kubernetes Job does not contain spec.template.spec") from error

    affinity = build_node_affinity(rule)

    if affinity:
        existing_affinity = pod_spec.setdefault("affinity", {})

        if not isinstance(existing_affinity, dict):
            raise ValueError(
                f"Job '{job.get('metadata', {}).get('name')}' affinity must be a YAML object"
            )

        existing_affinity["nodeAffinity"] = affinity["nodeAffinity"]

    if has_gpu_tag(rule):
        ensure_gpu_toleration(pod_spec)
        pod_spec["runtimeClassName"] = "nvidia"
        containers = pod_spec.get("containers", [])
        if containers and isinstance(containers[0], dict):
            ensure_gpu_resource(containers[0])


IMAGE_REPOSITORY_PLACEHOLDER = "__IMAGE_REPOSITORY__"


def read_image_repository(script_dir: Path) -> str:
    """Single source of truth for the registry/user images are tagged under (matches
    images.sh). EAER_IMAGE_REPOSITORY overrides for a one-off run; otherwise read from
    image-repository.conf, so changing the Docker Hub user/registry means editing one
    file, not hunting down every yaml that hardcodes it."""
    env_value = os.environ.get("EAER_IMAGE_REPOSITORY")
    if env_value:
        return env_value
    return (script_dir.parent / "images" / "image-repository.conf").read_text(
        encoding="utf-8"
    ).strip()


def rewrite_image_repository(node: Any, repository: str) -> None:
    """Recursively replace `image: __IMAGE_REPOSITORY__:tag` with the real repository, in
    place, regardless of where it's nested (plain Job containers, Argo Workflow templates,
    DAG tasks). Source manifests use the placeholder so they never need editing when the
    registry/user changes -- only image-repository.conf does."""
    if isinstance(node, dict):
        image = node.get("image")
        if isinstance(image, str) and image.startswith(IMAGE_REPOSITORY_PLACEHOLDER + ":"):
            node["image"] = repository + image[len(IMAGE_REPOSITORY_PLACEHOLDER):]
        for value in node.values():
            rewrite_image_repository(value, repository)
    elif isinstance(node, list):
        for item in node:
            rewrite_image_repository(item, repository)


def load_yaml_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def write_yaml_file(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as file:
        yaml.dump(
            data,
            file,
            Dumper=NoAliasSafeDumper,
            sort_keys=False,
            default_flow_style=False,
            allow_unicode=True,
        )


def get_argo_templates_container(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    kind = workflow.get("kind")

    if kind in ("Workflow", "WorkflowTemplate"):
        spec = workflow.get("spec", {})
    elif kind == "CronWorkflow":
        spec = workflow.get("spec", {}).get("workflowSpec", {})
    else:
        raise ValueError(
            f"Unsupported Argo kind '{kind}'. "
            "Expected Workflow, WorkflowTemplate, or CronWorkflow"
        )

    templates = spec.get("templates", [])

    if not isinstance(templates, list):
        raise ValueError("Argo spec.templates must be a list")

    return templates


def collect_argo_dag_tasks(
    templates: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    tasks_by_name: dict[str, list[dict[str, Any]]] = {}

    for template in templates:
        if not isinstance(template, dict):
            continue

        dag = template.get("dag")

        if not isinstance(dag, dict):
            continue

        tasks = dag.get("tasks", [])

        if not isinstance(tasks, list):
            continue

        for task in tasks:
            if not isinstance(task, dict):
                continue

            task_name = task.get("name")

            if not task_name:
                continue

            tasks_by_name.setdefault(slug(task_name), []).append(task)

    return tasks_by_name


def resolve_argo_template_from_rule(
    rule_name: str,
    templates_by_slug: dict[str, dict[str, Any]],
    tasks_by_slug: dict[str, list[dict[str, Any]]],
    warnings: list[str],
) -> tuple[str, dict[str, Any]] | None:
    """
    Batch behavior:

    1. Try to resolve the scheduling key as a DAG task name.
       If found, retrieve task.template and modify that referenced template.

    2. If no DAG task matches, try to resolve the key directly as a template name.

    This means the config can be written by task name while the generated
    YAML only modifies templates.
    """
    matching_tasks = tasks_by_slug.get(rule_name, [])

    if matching_tasks:
        referenced_template_names = {
            slug(str(task.get("template")))
            for task in matching_tasks
            if task.get("template")
        }

        if not referenced_template_names:
            warnings.append(
                f"Task scheduling key '{rule_name}' matched DAG task(s), "
                "but no referenced template was found"
            )
            return None

        if len(referenced_template_names) > 1:
            warnings.append(
                f"Task scheduling key '{rule_name}' matches multiple DAG tasks "
                f"referencing different templates: {sorted(referenced_template_names)}. "
                "Skipping because this is ambiguous."
            )
            return None

        template_slug = next(iter(referenced_template_names))
        template = templates_by_slug.get(template_slug)

        if not template:
            warnings.append(
                f"Task scheduling key '{rule_name}' references unknown template "
                f"'{template_slug}'"
            )
            return None

        if "container" not in template:
            warnings.append(
                f"Task scheduling key '{rule_name}' references template "
                f"'{template.get('name')}', but it is not a container template"
            )
            return None

        return template_slug, template

    direct_template = templates_by_slug.get(rule_name)

    if direct_template:
        if "container" not in direct_template:
            warnings.append(
                f"Scheduling key '{rule_name}' matches template "
                f"'{direct_template.get('name')}', but it is not a container template"
            )
            return None

        return rule_name, direct_template

    warnings.append(
        f"No matching Argo DAG task or container template found for scheduling key "
        f"'{rule_name}'"
    )

    return None


def apply_batch_rules(
    batch_pipeline_path: Path,
    output_path: Path,
    rules: dict[str, dict[str, list[str]]],
    image_repository: str,
    runtime: dict[str, Any] | None = None,
) -> list[str]:
    workflow = load_yaml_file(batch_pipeline_path)

    if not isinstance(workflow, dict):
        raise ValueError(f"{batch_pipeline_path} root must be a YAML object")

    rewrite_image_repository(workflow, image_repository)

    if runtime:
        apply_runtime_to_argo(workflow, runtime)
    apply_bert_matching_to_argo(workflow, bert_matching_enabled(runtime))

    templates = get_argo_templates_container(workflow)

    templates_by_slug = {
        slug(template.get("name", "")): template
        for template in templates
        if isinstance(template, dict) and template.get("name")
    }

    tasks_by_slug = collect_argo_dag_tasks(templates)

    warnings: list[str] = []

    modified_templates: dict[str, dict[str, Any]] = {}
    modified_templates_source: dict[str, str] = {}

    for rule_name, rule in rules.items():
        resolved = resolve_argo_template_from_rule(
            rule_name,
            templates_by_slug,
            tasks_by_slug,
            warnings,
        )

        if not resolved:
            continue

        template_slug, template = resolved
        template_display_name = template.get("name", template_slug)

        if template_slug in modified_templates:
            previous_rule = modified_templates[template_slug]
            previous_source = modified_templates_source[template_slug]

            if previous_rule == rule:
                continue

            warnings.append(
                f"Scheduling key '{rule_name}' wants to modify template "
                f"'{template_display_name}', but it was already modified by "
                f"'{previous_source}'. Keeping the first rule and skipping "
                f"'{rule_name}'."
            )
            continue

        apply_rule_to_argo_template(template, rule)

        modified_templates[template_slug] = copy.deepcopy(rule)
        modified_templates_source[template_slug] = rule_name

    write_yaml_file(output_path, workflow)

    return warnings


def job_candidate_names(path: Path, job: dict[str, Any]) -> set[str]:
    names = {slug(path.stem)}

    metadata_name = job.get("metadata", {}).get("name")

    if metadata_name:
        names.add(slug(metadata_name))

    expanded = set(names)

    for name in names:
        if name.startswith("kafka-"):
            expanded.add(name.removeprefix("kafka-"))

    return expanded


def apply_incremental_rules(
    input_dir: Path,
    output_dir: Path,
    rules: dict[str, dict[str, list[str]]],
    image_repository: str,
    runtime: dict[str, Any] | None = None,
) -> list[str]:
    warnings: list[str] = []
    applied_rules: set[str] = set()
    bert_matching = bert_matching_enabled(runtime)

    # exec/incremental is generated output. Recreate it so manifests removed or moved in
    # the source tree cannot survive as stale Jobs and be applied accidentally.
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for source_path in sorted(input_dir.rglob("*.yaml")):
        if source_path.name == BERT_MATCHING_JOB_FILE and not bert_matching:
            continue
        relative_path = source_path.relative_to(input_dir)
        destination_path = output_dir / relative_path
        document = load_yaml_file(source_path)

        if not isinstance(document, dict):
            warnings.append(f"Skipping '{relative_path}': YAML root is not an object")
            continue

        rewrite_image_repository(document, image_repository)

        if runtime and document.get("kind") == "Job":
            apply_runtime_to_job(document, runtime)
        if bert_matching and slug(str(document.get("metadata", {}).get("name", ""))) == "normalization":
            pod_spec = document["spec"]["template"]["spec"]
            add_bert_model_volume(pod_spec, pod_spec["containers"][0])

        if document.get("kind") != "Job":
            # Still write the parsed (and possibly image-rewritten) document rather than
            # a raw file copy, so the placeholder substitution above always applies.
            write_yaml_file(destination_path, document)
            continue

        candidates = job_candidate_names(source_path, document)
        matching_rule_name = next(
            (name for name in candidates if name in rules),
            None,
        )

        if matching_rule_name:
            apply_rule_to_kubernetes_job(document, rules[matching_rule_name])
            applied_rules.add(matching_rule_name)

        write_yaml_file(destination_path, document)

    for rule_name in sorted(rules):
        if rule_name == BERT_MATCHING_JOB and not bert_matching:
            continue
        if rule_name not in applied_rules:
            warnings.append(
                f"No matching Kubernetes Job manifest found for scheduling key "
                f"'{rule_name}'"
            )

    return warnings


def print_rules(title: str, rules: dict[str, dict[str, list[str]]]) -> None:
    print(title)

    if not rules:
        print("  No rules")
        return

    for name, rule in rules.items():
        print(f"  {name}")

        for field in FIELDS:
            values = ", ".join(rule[field]) if rule[field] else ""
            print(f"    {field:<8}: [{values}]")


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    k8s_dir = script_dir.parent

    parser = argparse.ArgumentParser(
        description=(
            "Generate scheduled Argo/Kubernetes manifests into "
            "k8s/pipeline/exec without modifying source manifests."
        )
    )

    parser.add_argument(
        "--config",
        default=str(script_dir / "scheduling.yaml"),
        help="Path to scheduling.yaml",
    )

    parser.add_argument(
        "--batch-input",
        default=str(k8s_dir / "pipeline" / "batch" / "pipeline.yaml"),
        help="Path to batch Argo pipeline.yaml",
    )

    parser.add_argument(
        "--incremental-input",
        default=str(k8s_dir / "pipeline" / "incremental"),
        help="Path to incremental manifests directory",
    )

    parser.add_argument(
        "--output",
        default=str(k8s_dir / "pipeline" / "exec"),
        help="Output directory",
    )

    parser.add_argument(
        "--mode",
        choices=("batch", "incremental", "all"),
        default="all",
        help="Which manifests to generate",
    )

    parser.add_argument(
        "--pipeline-mode",
        choices=(
            "embedding-training-inference-evaluation",
            "bert-training-evaluation",
        ),
        help="Limit plan rows to tasks executed by this business pipeline mode",
    )

    parser.add_argument(
        "--pipeline-config",
        help=(
            "Exact pipeline config being executed. Its random_walk.processes controls the "
            "random-walk Pod CPU allocation, embeddings_training.workers controls the "
            "Gensim Pod CPU allocation and bert_matching.enabled adds BERT training and "
            "the bert-matching worker."
        ),
    )

    parser.add_argument(
        "--print-rules",
        action="store_true",
        help="Print parsed scheduling rules",
    )

    parser.add_argument(
        "--print-plan",
        action="store_true",
        help="Print a compact per-task candidate summary",
    )

    parser.add_argument(
        "--plan-output",
        help="Write the scheduling plan as TSV",
    )

    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Generate the scheduling plan without writing executable manifests",
    )

    args = parser.parse_args()

    try:
        runtime = load_pipeline_runtime(
            Path(args.pipeline_config).resolve() if args.pipeline_config else None
        )
        config, plan = load_scheduling_bundle(
            Path(args.config).resolve(), k8s_dir / "results", runtime
        )
        if args.mode != "all":
            plan = [row for row in plan if row["phase"] == args.mode]
        plan = [
            row for row in plan
            if keep_plan_row(row, args.pipeline_mode, bert_matching_enabled(runtime))
        ]

        warnings: list[str] = []

        batch_rules = config.get("batch", {})
        incremental_rules = config.get("incremental", {})

        if args.print_rules:
            print_rules("Batch rules:", batch_rules)
            print()
            print_rules("Incremental rules:", incremental_rules)
            print()

        if args.print_plan:
            print_plan(plan)
        if args.plan_output:
            plan_path = Path(args.plan_output).resolve()
            write_plan(plan_path, plan)
            print(f"Scheduling plan saved: {plan_path}")
        if args.plan_only:
            return 0

        output_dir = Path(args.output).resolve()
        image_repository = read_image_repository(script_dir)

        if args.mode in ("batch", "all"):
            if batch_rules:
                warnings += apply_batch_rules(
                    Path(args.batch_input).resolve(),
                    output_dir / "batch" / "pipeline.yaml",
                    batch_rules,
                    image_repository,
                    runtime,
                )
            else:
                warnings.append("No 'batch' section found in scheduling configuration")

        if args.mode in ("incremental", "all"):
            if incremental_rules:
                warnings += apply_incremental_rules(
                    Path(args.incremental_input).resolve(),
                    output_dir / "incremental",
                    incremental_rules,
                    image_repository,
                    runtime,
                )
            else:
                warnings.append("No 'incremental' section found in scheduling configuration")

        print(f"Generated manifests in: {output_dir}")

        if warnings:
            print()
            print("Warnings:")

            for warning in warnings:
                print(f"  - {warning}")

    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
