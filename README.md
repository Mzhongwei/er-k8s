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

The original Python pipeline is split into multiple containerized tasks and orchestrated with Argo Workflows. The goal is to execute the entity resolution workflow on a Kubernetes cluster while keeping track of execution time, resource placement, and energy consumption through EcoFLOC or Alumet.

![Architecture overview](architecture.png)

## Table of contents

- Overview
- Main features
- Repository structure
- Prerequisites
- Quick start
- Dataset storage
- Usage
- Scheduling configuration
- Automatic EcoFLOC or Alumet measurement
- Useful commands
- Troubleshooting
- Acknowledgements

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
- Helper CLI wrapper through k8s/erctl.sh.

# Repository structure

```text
.
├── architecture.png
├── code/
│   └── Energy-Aware-Entity-Resolution/   # Original EAER Python project as a Git submodule
├── docker/                               # Dockerfiles for pipeline components
├── k8s/
│   ├── erctl.sh                          # Top-level management CLI
│   ├── scheduling/                       # Placement config, compiler, and runtime tools
│   ├── monitoring/
│   │   ├── ecofloc/                      # EcoFLOC collector and node access config
│   │   ├── alumet/                       # Alumet manager and Helm values
│   │   └── results.py                    # Persistent result collection and display
│   ├── images/                           # Image build tool and repository config
│   ├── pipeline/
│   │   ├── pipeline.sh                   # Pipeline lifecycle
│   │   ├── configmaps.sh                 # Runtime ConfigMap generation
│   │   ├── batch/                        # Source Argo Workflow manifest
│   │   ├── incremental/                  # Source incremental Kubernetes job manifests
│   │   └── exec/                         # Generated manifests, created by the compiler
│   ├── datasets/                         # Long-lived static dataset PV/PVC
│   ├── pvc-manifests/                    # PersistentVolumeClaim manifests
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

The control machine must be able to reach the Kubernetes API and the Argo namespace.

# Quick start

## 1. Clone the repository

```bash
git clone --recurse-submodules https://github.com/Mzhongwei/er-k8s.git --branch development
cd er-k8s
```

If the repository was already cloned without submodules:

```bash
git submodule update --init --recursive
```

## 2. Create local configuration

Copy the simulator and image repository examples, then edit the copies:

```bash
cp code/Energy-Aware-Entity-Resolution/services/dataStreamSimulator/src/main/resources/application.properties.example \
  code/Energy-Aware-Entity-Resolution/services/dataStreamSimulator/src/main/resources/application.properties

cp k8s/images/image-repository.conf.example \
  k8s/images/image-repository.conf

vim code/Energy-Aware-Entity-Resolution/services/dataStreamSimulator/src/main/resources/application.properties
vim k8s/images/image-repository.conf
```

Set `spring.kafka.bootstrap-servers` to the external broker, for example
`localhost:9092`. Set `image-repository.conf` to the image prefix used by the cluster,
for example `your-dockerhub-user/erctl`.

`image-repository.conf` is required even when images are reused: both the image tool and
the manifest compiler read it to produce complete image names. The two generated config
files are ignored by Git.

## 3. Prepare cluster resources

Create or select the Argo namespace:

```bash
kubectl create namespace argo --dry-run=client -o yaml | kubectl apply -f -
```

Apply the RBAC resources used by the helper scripts:

```bash
kubectl apply -n argo -f k8s/rbac-configmaps.yaml
```

`pipeline start` generates scheduled manifests and ConfigMaps automatically.

## 4. Configure the static dataset volume
Copy the simulator and image repository examples, then edit the copies:
```bash
cp k8s/datasets/dataset-volume.yaml.example k8s/datasets/dataset-volume.yaml
vim k8s/datasets/dataset-volume.yaml
```
Check nfs server ip address `{spec:{nfs:{server: <your nfs server ip>}}}` .
Check datasets path and configure `{spec:{nfs:{path: <your path>}}}` in file `k8s/datasets/dataset-volume.yaml`

Before applying the volume, verify the NFS `server` and `path` in
`k8s/datasets/dataset-volume.yaml`. All Kubernetes nodes that can run EAER Pods must be
able to mount that export.

For a fresh installation, apply it directly:

```bash
kubectl apply -f k8s/datasets/dataset-volume.yaml

kubectl wait -n argo \
  --for=jsonpath='{.status.phase}'=Bound \
  pvc/pipeline-data-claim \
  --timeout=5m

kubectl get pv eaer-datasets-pv
kubectl get pvc -n argo pipeline-data-claim
```

If `pipeline-data-claim` already exists with an incompatible dynamic definition, PVC
storage fields are immutable. Stop all Pods using it before replacing it:

```bash
kubectl get pods -n argo
kubectl delete pvc -n argo pipeline-data-claim --wait=true
kubectl delete pv eaer-datasets-pv --ignore-not-found --wait=true
kubectl apply -f k8s/datasets/dataset-volume.yaml
kubectl wait -n argo --for=jsonpath='{.status.phase}'=Bound \
  pvc/pipeline-data-claim --timeout=5m
```

Do not run the deletion block for an already-correct static claim or as part of every
experiment. The static dataset volume is retained across pipeline runs.

## 5. Prepare images

Build and push the complete image set when the images do not yet exist or their source code/Dockerfiles changed:

```bash
sudo docker login
bash k8s/erctl.sh images --build --push
```

When compatible tags already exist in the configured repository, skip this build step;
`pipeline start` will reuse/pull those tags. `docker login` is only needed for pushing.
A private registry additionally requires Kubernetes image-pull credentials on the cluster.

## 6. Check scheduling strategies

Use `k8s/scheduling/scheduling.yaml` as the only scheduling entry point. Select the
strategy there, then maintain node capabilities in `nodes.yaml`, task properties in
`workloads.yaml`, and data placement in `data.yaml`. Carbon-region confirmation is kept
separately in `carbon-intensity.yaml`.

## 7. Start and observe the pipeline

```bash
bash k8s/erctl.sh pipeline start \
  -c code/Energy-Aware-Entity-Resolution/config/examples/config-embedding.yaml \
  --results-summary
```

In another terminal, observe Jobs and Pods:

```bash
kubectl get jobs,pods -n argo -w
```

# Usage

Run with the helper script

The erctl wrapper can manage the pipeline from a single entry point:

```bash
bash k8s/erctl.sh pipeline start -c <config.yaml>
```

The config file's `mode` selects the embedding or BERT lifecycle.

The helper provides two different cancellation levels:

```bash
# Non-destructive cancellation: stop workloads, preserve PVCs and ConfigMaps.
bash k8s/erctl.sh pipeline stop

# Destructive reset: terminate workloads and recreate runtime PVCs.
bash k8s/erctl.sh pipeline terminate
```

`pipeline stop` is not a resumable pause. Incremental Jobs are deleted, but their
persistent state remains available for inspection or a later new run.

> Warning
> The pipeline helper is designed for an experimental cluster workflow. It may delete and recreate pipeline-related pods, workflows, PVCs, and PVs before starting a new run. Review k8s/pipeline/pipeline.sh before using it on a shared or production cluster.

# Scheduling configuration

Node placement is configured in:

```text
k8s/scheduling/scheduling.yaml
```

This remains the only file passed to the compiler and scheduling commands. It selects a
strategy and includes three focused files using paths relative to itself:

```yaml
version: 2
strategy: C3
preferences:
  allow_gpu_for_cpu: false
includes:
  nodes: nodes.yaml
  workloads: workloads.yaml
  data: data.yaml
```

The included files have distinct responsibilities:

- `nodes.yaml`: stable node capabilities and `schedulable` state;
- `workloads.yaml`: `batch` and `incremental` defaults and per-template task properties;
- `data.yaml`: storage class and node locations of shared data.

The loader rejects missing includes, extra top-level keys, and fields placed in the wrong
fragment; it does not silently merge duplicate definitions. Only the version 2 split
format is supported; the previous monolithic and compact rule formats are not accepted.
Select a strategy with one setting (`B0`, `C1` ... `C8`, `H1`, or `H2`). C8 is
Carbon-Aware Placement. A strategy list such as
`strategy: [C3, C7]` composes policies; optional `preferences.weights` controls their
relative importance. B0 applies no scoring policy and leaves placement to the Kubernetes
default scheduler among nodes not marked `schedulable: false`.

`C7` reads every usable `k8s/results/<run-id>/energy/summary.json`, normalizes each run's
`by_node_j` values, and averages the relative node energy index. It falls back to B0 with a
terminal warning when no usable history exists; manually entered historical-energy classes
are not required.

`C8` and H1 regenerate the node inventory in `k8s/scheduling/carbon-intensity.yaml` from
`kubectl get nodes` before compilation. Existing confirmed zone choices are preserved;
Kubernetes country/carbon labels and public-IP geolocation can suggest a zone, while private
or unresolved IPs are marked `NEEDS_CONFIRMATION`. The complete file and source links are
shown and the user must answer `yes` or `no` every time. `yes` continues; `no` opens
`$EDITOR` (or `vi`) and shows the edited file again. IP location is only a suggestion—the
zone must describe the node's physical electricity supply, not its VPN/NAT endpoint.

RTE éCO2mix supplies current FR intensity automatically. Other detected countries use
Electricity Maps and require `ELECTRICITY_MAPS_API_TOKEN`; when multiple countries are
present, FR is also queried through Electricity Maps so all nodes use a comparable data
method. A zone may instead define a
manual `intensity_g_per_kwh`, timestamp, and official `source_url`. C8 gives the lowest
current intensity the strongest Kubernetes node-affinity preference. Nodes in the same
electricity zone tie, and Kubernetes still applies resource, taint, and availability rules.

C8 needs read-only cluster access; verify it with `kubectl auth can-i list nodes`. Public-IP
geolocation sends that IP to the configured `ip_geolocation.source_url`; private IPs are
never sent and require labels, a preserved mapping, or manual editing.

Temporarily exclude one node from all EAER strategies, including B0:

```yaml
# k8s/scheduling/nodes.yaml
nodes:
  zhongwei-lap:
    schedulable: false
```

`pipeline start` compiles this configuration into executable manifests before creating
the workload.

Explain a placement decision without changing the cluster:

```bash
bash k8s/erctl.sh schedule recommend random-walk --group incremental
```

For H2, `--live` reads `kubectl top nodes`. H1 accepts current power measurements from a
YAML/JSON node mapping via `--metrics`. Runtime adaptation is dry-run by default:

```yaml
nodes:
  server2-labo: {power_watts: 210, carbon_intensity: medium}
```

```bash
bash k8s/erctl.sh schedule adapt random-walk --group incremental
bash k8s/erctl.sh schedule adapt random-walk --group incremental --apply
```

`--apply` is limited to tasks explicitly marked `migratable: true`; it recreates the
Kubernetes Job, so application state must be stored in a PVC/checkpoint.

The compiler writes generated manifests into:

```text
k8s/pipeline/exec/
```

The source manifests in k8s/pipeline/batch/ and k8s/pipeline/incremental/ are kept unchanged.

# Automatic energy measurement

Add arg `--energy-monitor` in the commande line to activate automatic energy measurement. For example,
```bash
bash k8s/erctl.sh pipeline start -c <config.yaml> --energy-monitor --results-summary
```

`--energy-monitor` keeps EcoFLOC as the default. Select a backend explicitly with:

```bash
# Existing behavior
bash k8s/erctl.sh pipeline start -c <config.yaml> \
  --energy-monitor ecofloc --results-summary

# Cluster-wide Alumet deployment
bash k8s/erctl.sh pipeline start -c <config.yaml> \
  --energy-monitor alumet --results-summary
```

## EcoFLOC [:link:](https://github.com/hhumbertoAv/ecofloc)

For installation instructions, see [:link:](https://github.com/hhumbertoAv/ecofloc)

When using `--energy-monitor`, copy and edit the node access list:

```bash
cp k8s/monitoring/ecofloc/energy-nodes.conf.example k8s/monitoring/ecofloc/energy-nodes.conf
vim k8s/monitoring/ecofloc/energy-nodes.conf
```

For every `ssh` node, first verify the host key with its administrator, connect once
interactively if needed, then verify non-interactive SSH and passwordless EcoFLOC:

```bash
ssh -o BatchMode=yes user@node true
```

Test a live PID on each node:

```bash
sleep 10 & PID=$!
sudo -n /usr/local/bin/ecofloc --cpu -p "$PID" -i 1000 -t 2
wait "$PID"
```
or
```bash
sleep 10 & PID=$!
sudo -n /bin/execute ecofloc --cpu -p "$PID" -i 1000 -t 2
wait "$PID"
```

The output must contain `Average Power` and `Total Energy` and must not contain
`CLOSED OR INEXISTENT`. The pipeline performs the same functional preflight and skips a node whose executable starts but cannot measure the PID.

The current EcoFLOC collector runs on the control host (`local`) or through SSH (`ssh` and
two-hop `vm`). Kubernetes API access alone is not host measurement access: a VM without an
SSH account is recorded as unavailable and needs either an SSH route or a separately
designed privileged host-PID collector. EcoFLOC output containing `PID ... CLOSED OR
INEXISTENT` and `0.00 Joules` is an invalid measurement; the preflight now rejects it.


## Alumet [:link:](https://github.com/alumet-dev)

Alumet is a persistent cluster service rather than a per-run child process. `erctl` checks that its node clients and InfluxDB are ready, records the pipeline's UTC time window, exports that window to `energy/alumet-raw.csv`, and writes the common `energy/summary.json`. Hardware and attributed energy are kept separate because adding them would double-count energy.
Attributed energy is split into EAER Pods found in `placement.tsv`, named system consumers, and consumers that Alumet could not map to a Pod (`unknown`). The raw InfluxDB export and the complete per-consumer breakdown remain available for auditing.


The chart creates a privileged, host-PID relay-client DaemonSet, read-only Kubernetes RBAC, the relay server, and persistent InfluxDB. This avoids per-run SSH and passwordless `sudo`, but a cluster administrator must authorize the one-time privileged deployment. RAPL must be available on each node; a VM whose hypervisor does not expose RAPL can report resource usage but not measured CPU energy. The values file does not enable NVML because enabling the chart's GPU resource limit on a mixed cluster would restrict the DaemonSet to GPU-capable nodes.

If the release uses a different namespace, bucket, organization, secret, or InfluxDB pod, set these before running `erctl`:

```bash
export ERCTL_ALUMET_NAMESPACE=alumet
export ERCTL_ALUMET_ORG=influxdata
export ERCTL_ALUMET_BUCKET=default
# Normally auto-detected; only set when the deployment uses nonstandard names:
export ERCTL_ALUMET_TOKEN_SECRET=eaer-alumet-influxdb2-auth
export ERCTL_ALUMET_INFLUX_POD=<influxdb-pod-name>
```

Create the local Helm values file and adjust its monitoring node and storage class:

```bash
cp k8s/monitoring/alumet/values.yaml.example k8s/monitoring/alumet/values.yaml
vim k8s/monitoring/alumet/values.yaml
```

Install Alumet once with the official chart before selecting that backend:

```bash
helm repo add alumet https://alumet-dev.github.io/helm-charts/
helm repo update
kubectl create namespace alumet --dry-run=client -o yaml | kubectl apply -f -
helm upgrade --install eaer-alumet alumet/alumet \
  --namespace alumet \
  -f k8s/monitoring/alumet/values.yaml

kubectl rollout status daemonset/eaer-alumet-alumet-relay-client \
  -n alumet --timeout=5m
kubectl get pods -n alumet -o wide
python3 k8s/monitoring/alumet/alumet.py preflight
```

Control continuous collection without deleting InfluxDB or its PVC:

```bash
# Label eligible nodes, start the relay server/clients, and enforce 7-day retention.
bash k8s/erctl.sh alumet start

# Show collector Pods, PVC/PV backing location, and bucket retention.
bash k8s/erctl.sh alumet status

# Stop relay clients/server. InfluxDB remains available with existing data.
bash k8s/erctl.sh alumet stop

# Change retention explicitly (7d is the project default).
bash k8s/erctl.sh alumet retention 7d
```

The InfluxDB process stores its database at `/var/lib/influxdb2` on the
`eaer-alumet-influxdb2` PVC. With `nfs-client`, the corresponding PV identifies the NFS server and dynamically provisioned directory; `erctl alumet status` prints that mapping. InfluxDB automatically expires raw points older than seven days. Every completed `--energy-monitor alumet` run first exports its selected window to
`k8s/results/<run-id>/energy/alumet-raw.csv` and `summary.json`; these exported files are not deleted by InfluxDB retention. A run that was never exported before its raw window expires cannot later reconstruct its Alumet report from InfluxDB.

Rebuild the categorized summary of an existing Alumet run without querying the cluster:

```bash
python3 k8s/monitoring/alumet/alumet.py summarize k8s/results/<run-id>
python3 k8s/monitoring/results.py show --root k8s/results --run <run-id>
```

# Data storage
## Datasets sources
The read-only dataset volume is defined in `k8s/datasets/dataset-volume.yaml`. Its current
NFS-to-Pod mapping is:

```text
NFS <ip>:/srv/nfs/k8s/data → Pod: /data
└── exp_datasets/           → Pod: /data/exp_datasets/
    ├── 2-fordors_zagats/
    └── 4-1_dirty_dblp_acm/
```

Kubernetes uses the static PV `eaer-datasets-pv` and PVC `pipeline-data-claim`; Pods mount
the PVC read-only at `/data`. Dataset paths in an experiment config therefore use the Pod
path, for example:

```yaml
data_source_A: "/data/exp_datasets/2-fordors_zagats/tableA.csv"
data_source_B: "/data/exp_datasets/2-fordors_zagats/tableB.csv"
ground_truth: "/data/exp_datasets/2-fordors_zagats/matches.txt"
```

The static dataset PVC is retained across runs. Runtime PVCs for buffers, models, graphs,
indexes, communication files, and predictions are mounted separately under `/app/data/*`
and are recreated by `pipeline start`.

## Results
All results, including, matching, Pod placement, and energy results if measured are stored under `k8s/results/<run-id>/`

Add `--results-archive <path-or-user@host:path>` when the run directory must also be copied
off the control node. Archive failure is reported separately and does not change workload status.


# Useful commands

Display available erctl commands:

```bash
bash k8s/erctl.sh help
```

Build or push Docker images:

```bash
bash k8s/erctl.sh images --build
bash k8s/erctl.sh images --push
# Actions can be combined; base is built before its component images.
bash k8s/erctl.sh images --build --push
```

The active image hierarchy is `kevinoulai/erctl:base` followed by the
component images (`normalization`, `graph`, `cgfeature`, `embedding`,
`featureindex`, `prediction`, `bert`, `kafka`, and
`kafka-producer`). The legacy `min`/`full` distinction is no longer used.
The `kafka` and `kafka-producer` images are clients of the external Kafka
broker configured in the pipeline YAML; this repository does not deploy a broker.

ConfigMaps are generated internally by `pipeline start` from the selected config file.

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

Regenerate the ConfigMaps and execution manifests by starting the pipeline again:

```bash
bash k8s/erctl.sh pipeline start -c <config.yaml>
```

GPU/BERT tasks stay pending

Check that the target node exposes GPU resources:

```bash
kubectl describe node <node-name> | grep -i nvidia
```

Also verify the scheduling rules in:

```text
k8s/scheduling/scheduling.yaml
k8s/scheduling/nodes.yaml
k8s/scheduling/workloads.yaml
k8s/scheduling/data.yaml
```

They are regenerated automatically on the next `pipeline start`.

Notes

This repository is an experimental deployment and orchestration layer around the Energy-Aware Entity Resolution pipeline. It is intended for research, testing, and infrastructure experimentation rather than direct production use.

# Acknowledgements

This project builds on an initial implementation developed by
[Kevin OULAI](https://github.com/kevin-oulai/k8s-python-llm/). The repository contains subsequent modifications, extensions, and maintenance work.
