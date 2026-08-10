# Energy-Aware Entity Resolution on Kubernetes

<p align="center">
  <img alt="Kubernetes" src="https://img.shields.io/badge/Kubernetes-orchestration-326CE5?logo=kubernetes&logoColor=white">
  <img alt="Argo Workflows" src="https://img.shields.io/badge/Argo-Workflows-FB6D3A?logo=argo&logoColor=white">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white">
  <img alt="Docker" src="https://img.shields.io/badge/Docker-images-2496ED?logo=docker&logoColor=white">
  <img alt="Bash" src="https://img.shields.io/badge/Bash-scripts-4EAA25?logo=gnubash&logoColor=white">
</p>

<p align="center">
  <img alt="MinIO" src="https://img.shields.io/badge/MinIO-object%20storage-C72E49?logo=minio&logoColor=white">
  <img alt="Apache Kafka" src="https://img.shields.io/badge/Apache%20Kafka-streaming-231F20?logo=apachekafka&logoColor=white">
  <img alt="NVIDIA GPU" src="https://img.shields.io/badge/NVIDIA-GPU%20optional-76B900?logo=nvidia&logoColor=white">
  <img alt="YAML" src="https://img.shields.io/badge/YAML-manifests-CB171E?logo=yaml&logoColor=white">
  <img alt="Status" src="https://img.shields.io/badge/status-research%20prototype-lightgrey">
</p>

This repository contains the Kubernetes deployment of an Energy-Aware Entity Resolution pipeline.

The original Python pipeline is split into multiple containerized tasks and orchestrated with Argo Workflows. The goal is to execute the entity resolution workflow on a Kubernetes cluster while keeping track of execution time, resource placement, and energy consumption through Ecofloc.

![Architecture overview](architecture.png)

## Table of contents

- Overview
- Main features
- Repository structure
- Prerequisites
- Installation
- Usage
- Scheduling configuration
- Process monitoring
- Useful commands
- Troubleshooting

# Overview

The project adapts an entity resolution pipeline to a distributed Kubernetes environment. Each major step of the pipeline is isolated into its own container or Kubernetes job, allowing the workflow to be scheduled, monitored, restarted, and measured more easily than in a single local Python execution.

The pipeline includes stages such as:

- data normalization;
- graph construction;
- random walks;
- embedding training;
- candidate enumeration;
- similarity calculation;
- collective graph feature extraction;
- decision making;
- evaluation;
- optional BERT-based processing on GPU-capable nodes.

Argo Workflows is used for batch execution, while a set of incremental Kubernetes manifests can be applied for inference-oriented executions.

# Main features

- Kubernetes-native execution of an entity resolution pipeline.
- Argo Workflow DAG for batch pipeline orchestration.
- Incremental execution manifests for inference and evaluation workflows.
- Dedicated Docker images for the different pipeline components.
- Persistent storage through PVC manifests for datasets, models, buffers, and reports.
- ConfigMap generation for Python distribution scripts and runtime configuration.
- Node scheduling compiler based on a readable YAML file.
- Ecofloc and process monitoring for energy and emissions reporting.
- Helper CLI wrapper through k8s/scripts/erctl.sh.

# Repository structure

```text
.
├── architecture.png
├── code/
│   └── Energy-Aware-Entity-Resolution/   # Original EAER Python project as a Git submodule
├── docker/                               # Dockerfiles for pipeline components
├── k8s/
│   ├── pipeline/
│   │   ├── batch/                        # Source Argo Workflow manifest
│   │   ├── incremental/                  # Source incremental Kubernetes job manifests
│   │   └── exec/                         # Generated manifests, created by the compiler
│   ├── pvc-manifests/                    # PersistentVolumeClaim manifests
│   ├── scripts/                          # Helper scripts and scheduling compiler
│   ├── svc-manifests/                    # Service manifests
│   └── rbac-configmaps.yaml              # RBAC resources for ConfigMap handling
├── archive/                              # Legacy / experimental files
└── README.md
```

# Prerequisites

The project assumes access to a working Kubernetes cluster.

Required tools:

- kubectl
- argo CLI
- docker
- bash
- python3

Cluster-side requirements:

- Argo Workflows installed in the argo namespace;
- a working storage provisioner for the PVCs;
- access to the Docker images referenced by the manifests;
- optional NVIDIA GPU support for BERT-related tasks.

When using the Git submodule, clone the repository recursively:

```bash
git clone --recurse-submodules https://github.com/kevin-oulai/k8s-python-llm.git
cd k8s-python-llm
```

If the repository was already cloned without submodules:

```bash
git submodule update --init --recursive
```

# Installation

Create or select the Argo namespace:

```bash
kubectl create namespace argo --dry-run=client -o yaml | kubectl apply -f -
```

Apply the RBAC resources used by the helper scripts:

```bash
kubectl apply -n argo -f k8s/rbac-configmaps.yaml
```

Generate the scheduled execution manifests:

```bash
bash k8s/scripts/erctl.sh compile
```

# Usage

Run with the helper script

The erctl wrapper can manage the pipeline from a single entry point:

```bash
bash k8s/scripts/erctl.sh pipeline start -m embedding
```

For BERT mode:

```bash
bash k8s/scripts/erctl.sh pipeline start -m bert
```

The helper script can also stop or terminate the latest workflow:

```bash
bash k8s/scripts/erctl.sh pipeline stop
bash k8s/scripts/erctl.sh pipeline terminate
```

> Warning
> The pipeline helper is designed for an experimental cluster workflow. It may delete and recreate pipeline-related pods, workflows, PVCs, and PVs before starting a new run. Review k8s/scripts/pipeline.sh before using it on a shared or production cluster.

# Scheduling configuration

Node placement is configured in:

```text
k8s/scripts/scheduling.yaml
```

Select a strategy with one setting (`B0`, `C1` ... `C7`, `H1`, or `H2`):

```yaml
version: 2
strategy: C3
```

`nodes` contains stable capabilities, `data` contains storage location, and task overrides
live under `batch.templates` / `incremental.templates`. A strategy list such as
`strategy: [C3, C7]` composes policies; optional `preferences.weights` controls their
relative importance. B0 writes no affinity and is the Kubernetes-default baseline.

Compile the scheduling configuration into executable manifests with:

```bash
bash k8s/scripts/erctl.sh compile
```

To inspect the parsed rules:

```bash
bash k8s/scripts/erctl.sh compile --print-rules
```

Explain a placement decision without changing the cluster:

```bash
bash k8s/scripts/erctl.sh schedule recommend random-walk --group incremental
```

For H2, `--live` reads `kubectl top nodes`. H1 accepts current power measurements from a
YAML/JSON node mapping via `--metrics`. Runtime adaptation is dry-run by default:

```yaml
nodes:
  server2-labo: {power_watts: 210, carbon_intensity: medium}
```

```bash
bash k8s/scripts/erctl.sh schedule adapt random-walk --group incremental
bash k8s/scripts/erctl.sh schedule adapt random-walk --group incremental --apply
```

`--apply` is limited to tasks explicitly marked `migratable: true`; it recreates the
Kubernetes Job, so application state must be stored in a PVC/checkpoint.

The compiler writes generated manifests into:

```text
k8s/pipeline/exec/
```

The source manifests in k8s/pipeline/batch/ and k8s/pipeline/incremental/ are kept unchanged.

# Automatic EcoFLOC measurement

Start EcoFLOC before pipeline Pods are created, drain it after all Jobs finish, and save
matching plus energy results under `k8s/results/<run-id>/`:

```bash
bash k8s/scripts/erctl.sh pipeline start -c <config.yaml> --energy-monitor --results-summary
```

Add `--results-archive <path-or-user@host:path>` when the run directory must also be copied
off the control node. Archive failure is reported separately and does not change workload status.

Show the latest saved run later:

```bash
bash k8s/scripts/erctl.sh pipeline results
```

# Useful commands

Display available erctl commands:

```bash
bash k8s/scripts/erctl.sh help
```

Build, load, or push Docker images:

```bash
bash k8s/scripts/erctl.sh images --build
bash k8s/scripts/erctl.sh images --load
bash k8s/scripts/erctl.sh images --push
# Actions can be combined; base is built before its component images.
bash k8s/scripts/erctl.sh images --build --push
```

The active image hierarchy is `kevinoulai/erctl:base` followed by the
component images (`normalization`, `graph`, `cgfeature`, `embedding`,
`featureindex`, `prediction`, `bert`, `config`, `kafka`, and
`kafka-producer`). The legacy `min`/`full` distinction is no longer used.
The `kafka` and `kafka-producer` images are clients of the external Kafka
broker configured in the pipeline YAML; this repository does not deploy a broker.

Create ConfigMaps and sync the dataset for the selected pipeline family. Both commands
default to `embedding`; dataset synchronization clears the data PVC and copies only the
three files required by that family.

```bash
# Embedding: tableA.csv, tableB.csv, matches.txt
bash k8s/scripts/erctl.sh configmaps
bash k8s/scripts/erctl.sh dataset

# BERT: train.csv, test.csv, valid.csv
bash k8s/scripts/erctl.sh configmaps bert
bash k8s/scripts/erctl.sh dataset bert
```

Check Argo workflows:

```bash
kubectl get wf -n argo
argo list -n argo
```

Check pods and logs:

```bash
kubectl get pods -n argo
argo logs -n argo @latest
kubectl logs -n argo <pod-name>
```

Check persistent volumes and claims:

```bash
kubectl get pvc -n argo
kubectl get pv
```

# Troubleshooting

The submodule directory is empty

Run:

```bash
git submodule update --init --recursive
```

Pods stay pending because of PVC errors

Check that the PVCs exist and that the cluster has a working storage provisioner:

```bash
kubectl get pvc -n argo
kubectl describe pvc -n argo <pvc-name>
```

Then reapply the manifests if needed:

```bash
kubectl apply -n argo -f k8s/pvc-manifests/
```

Argo cannot find ConfigMaps

Regenerate the ConfigMaps:

```bash
bash k8s/scripts/erctl.sh configmaps embedding
```

or, for BERT mode:

```bash
bash k8s/scripts/erctl.sh configmaps bert
```

GPU/BERT tasks stay pending

Check that the target node exposes GPU resources:

```bash
kubectl describe node <node-name> | grep -i nvidia
```

Also verify the scheduling rules in:

```text
k8s/scripts/scheduling.yaml
```

Then regenerate the execution manifests:

```bash
bash k8s/scripts/erctl.sh compile
```

Notes

This repository is an experimental deployment and orchestration layer around the Energy-Aware Entity Resolution pipeline. It is intended for research, testing, and infrastructure experimentation rather than direct production use.

#  Configuration
`code/Energy-Aware-Entity-Resolution/services/dataStreamSimulator/src/main/resources/application.properties.example` -->  `code/Energy-Aware-Entity-Resolution/services/dataStreamSimulator/src/main/resources/application.properties`


`k8s/scripts/image-repository.conf.example`--> `k8s/scripts/image-repository.conf` and change the dockerhub username

## Acknowledgements

This project builds on an initial implementation developed by
[Kevin OULAI](https://github.com/kevin-oulai/k8s-python-llm/). The repository contains subsequent modifications, extensions, and maintenance work.
