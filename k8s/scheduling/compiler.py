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


class LiteralString(str):
    pass


class NoAliasSafeDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


def literal_str_representer(dumper: yaml.Dumper, data: LiteralString):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


NoAliasSafeDumper.add_representer(LiteralString, literal_str_representer)




def load_scheduling_bundle(
    path: Path, results_dir: Path
) -> tuple[dict[str, dict[str, dict[str, list[str]]]], list[dict[str, Any]]]:
    data = load_policy_config(path)
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
) -> list[str]:
    workflow = load_yaml_file(batch_pipeline_path)

    if not isinstance(workflow, dict):
        raise ValueError(f"{batch_pipeline_path} root must be a YAML object")

    rewrite_image_repository(workflow, image_repository)

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
) -> list[str]:
    warnings: list[str] = []
    applied_rules: set[str] = set()

    # exec/incremental is generated output. Recreate it so manifests removed or moved in
    # the source tree cannot survive as stale Jobs and be applied accidentally.
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for source_path in sorted(input_dir.rglob("*.yaml")):
        relative_path = source_path.relative_to(input_dir)
        destination_path = output_dir / relative_path
        document = load_yaml_file(source_path)

        if not isinstance(document, dict):
            warnings.append(f"Skipping '{relative_path}': YAML root is not an object")
            continue

        rewrite_image_repository(document, image_repository)

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
        config, plan = load_scheduling_bundle(
            Path(args.config).resolve(), k8s_dir / "results"
        )
        if args.mode != "all":
            plan = [row for row in plan if row["phase"] == args.mode]
        if args.pipeline_mode == "embedding-training-inference-evaluation":
            plan = [
                row for row in plan
                if row["phase"] == "incremental" or not row["task"].startswith("bert-")
            ]
        elif args.pipeline_mode == "bert-training-evaluation":
            plan = [
                row for row in plan
                if row["phase"] == "batch" and row["task"].startswith("bert-")
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
