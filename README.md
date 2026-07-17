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
  <img alt="Redpanda Kafka" src="https://img.shields.io/badge/Redpanda%20%2F%20Kafka-streaming-D71920?logo=apachekafka&logoColor=white">
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
- Persistent storage through PVC manifests for datasets, models, buffers, Kafka data, and reports.
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
│   ├── kafka/                            # Kafka / Redpanda-related manifests
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

The file defines scheduling rules for both batch and incremental executions. Each task can use the following fields:

```yaml
tags: []       # Logical task tags, for example GPU-related tasks
prefer: []     # Preferred Kubernetes nodes
fallback: []   # Fallback nodes if preferred nodes are unavailable
avoid: []      # Nodes to avoid
```

Compile the scheduling configuration into executable manifests with:

```bash
bash k8s/scripts/erctl.sh compile
```

To inspect the parsed rules:

```bash
bash k8s/scripts/erctl.sh compile --print-rules
```

The compiler writes generated manifests into:

```text
k8s/pipeline/exec/
```

The source manifests in k8s/pipeline/batch/ and k8s/pipeline/incremental/ are kept unchanged.

# Process monitoring with EcoFloc

The repository provides a process-level monitoring command through erctl:

bash k8s/scripts/erctl.sh process ecofloc

This command detects Python processes running pipeline scripts from /app/... on the configured Kubernetes nodes, then launches EcoFloc on the node where each target PID is running. This is useful when the objective is to observe the actual processes executed by the Argo workflow instead of relying only on workflow-level reports.

The process helper has two main modes:

bash k8s/scripts/erctl.sh process get
bash k8s/scripts/erctl.sh process ecofloc
get lists the detected Python PIDs and their commands.
ecofloc monitors the detected PIDs with EcoFloc.

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
The external Kafka deployment does not require a broker image; add
`--with-kafka-server` only when the optional local Redpanda image is needed.

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

## Acknowledgements

This project builds on an initial implementation developed by
[Kevin OULAI](https://github.com/kevin-oulai/k8s-python-llm/). The repository contains subsequent modifications, extensions, and maintenance work.
