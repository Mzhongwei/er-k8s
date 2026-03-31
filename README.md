# EAER Kubernetes Deployment Guide

This root README is focused on Kubernetes deployment and operations.

## 1. Scope

This repository runs an Energy-Aware Entity Resolution workflow as multiple Kubernetes services.

Main folders:

- `k8s/`: deployments, services, storage manifests, operational scripts
- `code/python_files/`: Python scripts mounted into pods through ConfigMaps

## Architecture

![EAER architecture](architecture.png)

[Open architecture image](architecture.png)

## 2. Prerequisites

Required tools:

- `kubectl`
- `minikube`
- `docker`
- Bash shell (Git Bash, WSL, or Linux shell)

Default runtime values used by scripts:

- namespace: `erctl`
- minikube profile: `domolandes`
- container image: `erctl:slim`

## 3. Quick Start (K8s)

Run commands from repository root.

### Step 1. Build the runtime image

```bash
docker build -t erctl:slim .
```

### Step 2. Deploy all EAER resources

```bash
bash k8s/scripts/erctl.sh start -M
```

`-M` starts Minikube automatically if needed.

### Step 3. Validate deployment

```bash
bash k8s/scripts/erctl.sh test -r
```

### Step 4. Check logs and metrics

```bash
bash k8s/scripts/erctl.sh logs -a -f
bash k8s/scripts/erctl.sh metrics
```

## 4. What start does

`erctl start` performs:

1. Ensures Minikube is running (if `-M` is used).
2. Creates namespace `erctl`.
3. Applies all service manifests from `k8s/services/`.
4. Creates one ConfigMap per Python file from `code/python_files/*.py`.
5. Applies persistent volumes from `k8s/persistent-volumes/`.
6. Applies persistent volume claims from `k8s/persistent-volume-claims/`.
7. Applies all deployments from `k8s/deployments/`.

Important: ConfigMaps are generated from scripts in `code/python_files/`, not from dedicated ConfigMap YAML files.

## 5. Operational Commands

Main command wrapper:

```bash
bash k8s/scripts/erctl.sh help
```

Tip (Bash): add this to your `~/.bashrc` to call `erctl` directly.

```bash
export EAER_ROOT="/path/to/k8s-python-llm"
alias erctl='bash "$EAER_ROOT/k8s/scripts/erctl.sh"'
```

Then reload your shell:

```bash
source ~/.bashrc
```

After that, you can run:

```bash
erctl start -M
erctl test -a
erctl logs -a
```

Lifecycle:

```bash
bash k8s/scripts/erctl.sh start
bash k8s/scripts/erctl.sh stop
bash k8s/scripts/erctl.sh restart -M
```

Testing:

```bash
# resource checks
bash k8s/scripts/erctl.sh test -r

# script behavior checks (via logs)
bash k8s/scripts/erctl.sh test -s

# both
bash k8s/scripts/erctl.sh test -a
```

Logs:

```bash
# all EAER pods
bash k8s/scripts/erctl.sh logs -a

# one app label, example: normalization
bash k8s/scripts/erctl.sh logs -n normalization
```

Metrics:

```bash
bash k8s/scripts/erctl.sh metrics
bash k8s/scripts/erctl.sh metrics -s memory -o desc -f table
bash k8s/scripts/erctl.sh metrics -s cpu -o desc -f csv
```

## 6. K8s Resource Map

Deployments are defined in `k8s/deployments/`:

- normalization
- graph-construction
- random-walk
- embedding-training
- cg-feature-extraction
- feature-index-construction
- candidate-enumeration
- calculating-similarity
- decision-making
- BERT-training
- BERT-inference

Services are defined in `k8s/services/` for network-facing components.

Storage manifests:

- PVs: `k8s/persistent-volumes/`
- PVCs: `k8s/persistent-volume-claims/`

## 7. Troubleshooting

### Minikube not running

Use:

```bash
bash k8s/scripts/erctl.sh start -M
```

### Pods stuck in ImagePullBackOff or ErrImageNeverPull

Deployments use `imagePullPolicy: Never`, so the image must exist in Minikube:

```bash
docker build -t erctl:slim .
```

Then restart:

```bash
bash k8s/scripts/erctl.sh restart
```

### No metrics output

`metrics.sh` tries to enable Minikube metrics-server automatically.
If it still fails, run:

```bash
minikube addons enable metrics-server -p domolandes
```

### Service connectivity issues

Run:

```bash
bash k8s/scripts/erctl.sh test -r
bash k8s/scripts/erctl.sh logs -a
kubectl get pods,svc -n erctl
```

## 8. Proof of Concept Manifests

Manual POC files and commands are available in:

- `k8s/proof-of-concept/`
- `k8s/proof-of-concept/commands.txt`
