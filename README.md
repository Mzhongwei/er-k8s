# Energy-Aware Entity Resolution on Kubernetes

This repository contains a Kubernetes and Argo Workflows deployment of the Energy-Aware Entity Resolution pipeline. It also includes the Python code for the pipeline itself, container build files, and helper scripts for image builds, ConfigMaps, datasets, and workflow report retrieval.

## What This Repo Does

- Builds container images for the EAER pipeline stages.
- Deploys the cluster-side services and workloads used by the pipeline.
- Runs the Argo workflow defined in `k8s/argo/pipeline.yaml`.
- Tracks CodeCarbon emissions per pod and aggregates them into a shared CSV report.
- Fetches the report back to your local machine with `erctl fetch-report`.

Pipeline schema illustrating the main components and data flow:

![Pipeline Schema](architecture.png)

## Repository Map

- `code/Energy-Aware-Entity-Resolution/` - main EAER Python project.
- `k8s/argo/` - Argo workflow and PVC manifests.
- `k8s/deployments/` - Kubernetes Deployment manifests.
- `k8s/services/` - Kubernetes Service manifests.
- `k8s/scripts/` - helper scripts and the `erctl` wrapper.
- `docker/` - Dockerfiles for the pipeline images.
- `archive/` - legacy manifests and earlier proof-of-concept material.

## Prerequisites

- `kubectl`
- `argo` CLI if you submit workflows directly
- `docker`
- `bash`

If you are working locally with Minikube, the helper scripts also expect a running Minikube profile and the PVCs defined under `k8s/argo/pvc-manifests/`.

## Common Workflows

### 1. Generate or refresh ConfigMaps

```bash
bash k8s/scripts/erctl.sh configmaps embedding
```

Use `bert` instead of `embedding` if you are updating the BERT-oriented config set.

### 2. Sync the BERT dataset PVC

```bash
bash k8s/scripts/erctl.sh dataset
```

### 3. Build images (should not be needed)

```bash
bash k8s/scripts/erctl.sh images --build
```

### 4. Submit an Argo workflow

```bash
argo submit k8s/argo/pipeline.yaml -n argo --watch
```

### 5. Fetch the CodeCarbon report

```bash
bash k8s/scripts/erctl.sh fetch-report --workflow <workflow-name>
```

By default the helper writes the file into `./reports/` as `emissions-<workflow-name>.csv`. Use `--dest <path>` if you want the CSV in a different location.

## Emissions Reporting

Each decorated pipeline stage writes CodeCarbon output into shared storage and the repository aggregates that data into a per-workflow CSV report. The CSV includes:

- emissions
- total energy consumed
- CPU, GPU, and RAM energy
- CPU, GPU, and RAM power
- per-pod metadata and timestamps

The current report path is mounted under `/app/reports/codecarbon/<workflow-name>/` in the workflow pods, and the fetch helper copies the final CSV from there.

## Helpful Commands

```bash
bash k8s/scripts/erctl.sh help
bash k8s/scripts/erctl.sh configmaps --help
bash k8s/scripts/erctl.sh dataset --help
```

```bash
kubectl get wf -n argo
kubectl get pods -n argo
kubectl get pvc -n argo | grep reports
```

## Troubleshooting

- If a workflow report is missing, verify the workflow completed and that the `pipeline-reports-claim` PVC exists.
- If you need to refresh the helpers after editing the Python code, rerun the ConfigMap generation step.
- If Argo cannot mount the report path, check the workflow template mounts in `k8s/argo/pipeline.yaml`.

## Code

For the pipeline implementation itself, see the submodule [Energy-Aware-Entity-Resolution](https://github.com/kevin-oulai/Energy-Aware-Entity-Resolution/tree/distribution-kubernetes)