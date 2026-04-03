# Energy-Aware Entity Resolution on Kubernetes

This repository packages an Energy-Aware Entity Resolution (EAER) workflow for Kubernetes and Argo Workflows.

## Architecture

![EAER architecture](architecture.png)

[Open architecture image](architecture.png)

## Repository Layout

- `code/Energy-Aware-Entity-Resolution/`: core EAER Python codebase (`main_distribution.py`, pipeline modules, models, configs).
- `code/python_files/`: service-oriented Python entrypoints used by Kubernetes components.
- `docker/`: Dockerfiles for component images (`Dockerfile.manager`, `Dockerfile.normalization`, `Dockerfile.bert`, plus base images).
- `k8s/deployments/`: Kubernetes Deployment manifests.
- `k8s/services/`: Kubernetes Service manifests.
- `k8s/argo/pipeline.yaml`: Argo workflow DAG for pipeline orchestration.
- `k8s/scripts/`: operational scripts (`erctl.sh`, `start.sh`, `stop.sh`, `test.sh`, `logs.sh`, `metrics.sh`, `images.sh`).

## Prerequisites

- `kubectl`
- `minikube`
- `docker`
- Bash shell (`Git Bash`, `WSL`, or Linux shell)

Default runtime values used by scripts:

- Namespace: `erctl`
- Minikube profile: `domolandes` (or `EAER_MINIKUBE_PROFILE`)

## Quick Start (Kubernetes)

Run from repository root.

1. Build images:

```bash
bash k8s/scripts/erctl.sh images --build
```

2. Deploy resources (`-M` starts Minikube if needed):

```bash
bash k8s/scripts/erctl.sh start -M
```

3. Validate resources:

```bash
bash k8s/scripts/erctl.sh test -r
```

4. Inspect logs and metrics:

```bash
bash k8s/scripts/erctl.sh logs -a
bash k8s/scripts/erctl.sh metrics
```

## What `start` Currently Applies

`bash k8s/scripts/erctl.sh start` currently:

1. Verifies Minikube is running (or starts it with `-M`).
2. Creates namespace `erctl` if missing.
3. Applies all manifests in `k8s/services/*.yaml`.
4. Applies all manifests in `k8s/deployments/*.yaml`.

Important current state:

- ConfigMap generation from `code/python_files/*.py` is present in script comments but disabled.
- Persistent volumes and claims application is present in script comments but disabled.
- Only `.yaml` files in `k8s/deployments/` are applied. Files ending in `.DISABLED` are not applied.

## Enabled vs Disabled Deployments

Enabled by default (`*.yaml`):

- `deployment-manager.yaml`
- `deployment-normalization.yaml`
- `deployment-BERT.yaml`

Present but disabled (`*.DISABLED`):

- `deployment-calculating-similarity.yaml.DISABLED`
- `deployment-candidate-enumeration.yaml.DISABLED`
- `deployment-cg-feature-extraction.yaml.DISABLED`
- `deployment-decision-making.yaml.DISABLED`
- `deployment-embedding-training.yaml.DISABLED`
- `deployment-evaluation.yaml.DISABLED`
- `deployment-feature-index-construction.yaml.DISABLED`
- `deployment-graph-construction.yaml.DISABLED`
- `deployment-random-walk.yaml.DISABLED`

## Services

The active service manifests are:

- `k8s/services/service-manager.yaml`
- `k8s/services/service-normalization.yaml`
- `k8s/services/service-bert.yaml`

`service-manager` exposes ports `5000-5012` for pipeline stage endpoints.

## `erctl` Command Reference

Main entrypoint:

```bash
bash k8s/scripts/erctl.sh help
```

Common commands:

```bash
# lifecycle
bash k8s/scripts/erctl.sh start -M
bash k8s/scripts/erctl.sh stop -M
bash k8s/scripts/erctl.sh restart -M

# tests
bash k8s/scripts/erctl.sh test -r
bash k8s/scripts/erctl.sh test -s
bash k8s/scripts/erctl.sh test -a

# logs
bash k8s/scripts/erctl.sh logs -a
bash k8s/scripts/erctl.sh logs -n normalization -f

# metrics
bash k8s/scripts/erctl.sh metrics -s memory -o desc -f table

# images
bash k8s/scripts/erctl.sh images --build
bash k8s/scripts/erctl.sh images --push
```

## Argo Workflow

Argo workflow manifest:

- `k8s/argo/pipeline.yaml`

Highlights:

- Uses DAG orchestration with conditional branches based on `mode`.
- Includes `pipeline-init` step that runs `kevinoulai/erctl:config` and extracts mode via `scripts/extract-config-mode.sh`.
- Embedding tasks and BERT tasks are conditionally executed through `when` expressions.
- Current task containers are mostly placeholder BusyBox commands for pipeline wiring validation.

Example submit command:

```bash
argo submit k8s/argo/pipeline.yaml -n argo -p mode=bert-training-b_evaluation -p raw_data=source_data --watch
```

## Troubleshooting

### Minikube profile is not running

Ensure the terminal is in administrator mode.
If Minikube profile `domolandes` is not running, start it with:

```bash
bash k8s/scripts/erctl.sh start -M
```

### Namespace/resources check

```bash
kubectl get ns erctl
kubectl get pods,svc -n erctl
```

### Metrics unavailable

```bash
minikube addons enable metrics-server -p domolandes
bash k8s/scripts/erctl.sh metrics
```

### Validate manifests quickly

```bash
bash k8s/scripts/erctl.sh test -r
```

## Proof of Concept Manifests

- `k8s/proof-of-concept/`
- `k8s/proof-of-concept/commands.txt`
