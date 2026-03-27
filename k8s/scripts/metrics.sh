#!/bin/bash

# Re-run with bash if invoked from another shell (e.g., sh).
if [ -z "${BASH_VERSION:-}" ]; then
	exec bash "$0" "$@"
fi

set -euo pipefail

SORT_BY="pod"
ORDER="asc"
FORMAT="table"

usage() {
	cat << 'EOF'
Usage: eaer-k8s metrics [options]

Extract pod consumption metrics from Kubernetes and sort them.

Options:
  -s, --sort FIELD          Sort field: pod|cpu|memory (default: pod)
  -o, --order DIR           Sort order: asc|desc (default: asc)
  -f, --format TYPE         Output format: table|csv|tsv (default: table)
  -h, --help, -help, help   Show this help

Notes:
  - Data comes from the Metrics API (kubectl top pods).
  - CPU is shown in cores or millicores; memory in bytes units.
EOF
}

require_cmd() {
	local cmd="$1"
	if ! command -v "$cmd" >/dev/null 2>&1; then
		echo "Required command not found: $cmd"
		exit 1
	fi
}

while [ $# -gt 0 ]; do
	case "$1" in
		-s|--sort)
			if [ $# -lt 2 ]; then
				echo "Option $1 requires a value."
				exit 1
			fi
			SORT_BY="$2"
			shift
			;;
		-o|--order)
			if [ $# -lt 2 ]; then
				echo "Option $1 requires a value."
				exit 1
			fi
			ORDER="$2"
			shift
			;;
		-f|--format)
			if [ $# -lt 2 ]; then
				echo "Option $1 requires a value."
				exit 1
			fi
			FORMAT="$2"
			shift
			;;
		-h|--help|-help|help)
			usage
			exit 0
			;;
		*)
			echo "Unknown option: $1"
			usage
			exit 1
			;;
	esac
	shift
done

case "$SORT_BY" in
	pod|cpu|memory) ;;
	*)
		echo "Invalid sort field: $SORT_BY (allowed: pod|cpu|memory)"
		exit 1
		;;
esac

case "$ORDER" in
	asc|desc) ;;
	*)
		echo "Invalid sort order: $ORDER (allowed: asc|desc)"
		exit 1
		;;
esac

case "$FORMAT" in
	table|csv|tsv) ;;
	*)
		echo "Invalid format: $FORMAT (allowed: table|csv|tsv)"
		exit 1
		;;
esac

require_cmd kubectl
require_cmd awk
require_cmd sort

top_args=(top pods --all-namespaces --no-headers)
last_top_error=""

fetch_top_metrics() {
	local output
	if output="$(kubectl "${top_args[@]}" 2>&1)"; then
		last_top_error=""
		printf '%s' "$output"
		return 0
	fi
	last_top_error="$output"
	return 1
}

wait_for_metrics_api() {
	local attempts=12
	local sleep_seconds=5
	local i
	for ((i=1; i<=attempts; i++)); do
		if kubectl get apiservice v1beta1.metrics.k8s.io >/dev/null 2>&1; then
			if fetch_top_metrics >/dev/null 2>&1; then
				return 0
			fi
		fi
		sleep "$sleep_seconds"
	done
	return 1
}

print_metrics_diagnostics() {
	echo "Diagnostics:"
	echo "- kubectl top pods error:"
	if [ -n "$last_top_error" ]; then
		echo "  $last_top_error"
	else
		echo "  (no error details captured)"
	fi

	local api_status
	api_status="$(kubectl get apiservice v1beta1.metrics.k8s.io -o jsonpath='{range .status.conditions[*]}{.type}={.status}:{.reason} {.message}{"\\n"}{end}' 2>/dev/null || true)"
	if [ -n "$api_status" ]; then
		echo "- APIService v1beta1.metrics.k8s.io conditions:"
		while IFS= read -r line; do
			echo "  $line"
		done <<< "$api_status"
	else
		echo "- APIService v1beta1.metrics.k8s.io conditions: unavailable"
	fi

	echo "- metrics-server pods (kube-system):"
	kubectl get pods -n kube-system -l k8s-app=metrics-server --no-headers 2>/dev/null | awk '{print "  "$1"\t"$3"\t"$2}' || true

	echo "- metrics-server deployment (kube-system):"
	kubectl get deploy metrics-server -n kube-system -o wide 2>/dev/null | sed '1d' | awk '{print "  "$0}' || true
}

raw_metrics=""
if ! raw_metrics="$(fetch_top_metrics)"; then
	profile="${EAER_MINIKUBE_PROFILE:-domolandes}"

	if command -v minikube >/dev/null 2>&1 && minikube status -p "$profile" --format '{{.Host}}' 2>/dev/null | grep -q '^Running$'; then
		echo "Metrics API not ready. Enabling Minikube addon 'metrics-server' on profile '$profile'..."
		if minikube addons enable metrics-server -p "$profile" >/dev/null 2>&1; then
			echo "Waiting for metrics-server to become available..."
			if wait_for_metrics_api && raw_metrics="$(fetch_top_metrics)"; then
				echo "Metrics API is available."
			else
				echo "Unable to fetch pod metrics after enabling metrics-server."
				echo "Check addon status with: minikube addons list -p $profile"
				print_metrics_diagnostics
				exit 1
			fi
		else
			echo "Failed to enable minikube addon 'metrics-server'."
			print_metrics_diagnostics
			exit 1
		fi
	else
		echo "Unable to fetch pod metrics via 'kubectl top pods'."
		echo "Make sure Metrics Server is installed and cluster metrics are available."
		echo "For Minikube: minikube addons enable metrics-server -p ${profile}"
		print_metrics_diagnostics
		exit 1
	fi
fi

if [ -z "$raw_metrics" ]; then
	echo "No pod metrics found for the current filters."
	exit 0
fi

normalized="$(printf '%s\n' "$raw_metrics" | awk '
function cpu_to_m(v) {
	if (v ~ /m$/) {
		sub(/m$/, "", v)
		return v + 0
	}
	return (v + 0) * 1000
}
function mem_to_b(v, num, unit) {
	if (v ~ /Ki$/) { sub(/Ki$/, "", v); return (v + 0) * 1024 }
	if (v ~ /Mi$/) { sub(/Mi$/, "", v); return (v + 0) * 1024 * 1024 }
	if (v ~ /Gi$/) { sub(/Gi$/, "", v); return (v + 0) * 1024 * 1024 * 1024 }
	if (v ~ /Ti$/) { sub(/Ti$/, "", v); return (v + 0) * 1024 * 1024 * 1024 * 1024 }
	if (v ~ /K$/)  { sub(/K$/, "", v);  return (v + 0) * 1000 }
	if (v ~ /M$/)  { sub(/M$/, "", v);  return (v + 0) * 1000 * 1000 }
	if (v ~ /G$/)  { sub(/G$/, "", v);  return (v + 0) * 1000 * 1000 * 1000 }
	if (v ~ /T$/)  { sub(/T$/, "", v);  return (v + 0) * 1000 * 1000 * 1000 * 1000 }
	return v + 0
}
{
	if (NF == 4) {
		namespace = $1
		pod = $2
		cpu = $3
		mem = $4
	} else if (NF == 3) {
		namespace = "-"
		pod = $1
		cpu = $2
		mem = $3
	} else {
		next
	}

	cpu_m = cpu_to_m(cpu)
	mem_b = mem_to_b(mem)
	printf "%s\t%s\t%s\t%s\t%.0f\t%.0f\n", namespace, pod, cpu, mem, cpu_m, mem_b
}')"

if [ -z "$normalized" ]; then
	echo "No pod metrics found for the current filters."
	exit 0
fi

sort_reverse=""
if [ "$ORDER" = "desc" ]; then
	sort_reverse="-r"
fi

case "$SORT_BY" in
	pod)
		if [ "$ORDER" = "desc" ]; then
			sorted="$(printf '%s\n' "$normalized" | sort -t $'\t' -k2,2r -k1,1r)"
		else
			sorted="$(printf '%s\n' "$normalized" | sort -t $'\t' -k2,2 -k1,1)"
		fi
		;;
	cpu)
		if [ "$ORDER" = "desc" ]; then
			sorted="$(printf '%s\n' "$normalized" | sort -t $'\t' -k5,5nr -k2,2 -k1,1)"
		else
			sorted="$(printf '%s\n' "$normalized" | sort -t $'\t' -k5,5n -k2,2 -k1,1)"
		fi
		;;
	memory)
		if [ "$ORDER" = "desc" ]; then
			sorted="$(printf '%s\n' "$normalized" | sort -t $'\t' -k6,6nr -k2,2 -k1,1)"
		else
			sorted="$(printf '%s\n' "$normalized" | sort -t $'\t' -k6,6n -k2,2 -k1,1)"
		fi
		;;
esac

if [ "$FORMAT" = "csv" ]; then
	echo "namespace,pod,cpu,memory"
	while IFS=$'\t' read -r ns pod cpu mem _ _; do
		printf "%s,%s,%s,%s\n" "$ns" "$pod" "$cpu" "$mem"
	done <<< "$sorted"
elif [ "$FORMAT" = "tsv" ]; then
	echo -e "namespace\tpod\tcpu\tmemory"
	while IFS=$'\t' read -r ns pod cpu mem _ _; do
		printf "%s\t%s\t%s\t%s\n" "$ns" "$pod" "$cpu" "$mem"
	done <<< "$sorted"
else
	printf "%-25s %-45s %12s %14s\n" "NAMESPACE" "POD" "CPU" "MEMORY"
	printf "%-25s %-45s %12s %14s\n" "---------" "---" "---" "------"
	while IFS=$'\t' read -r ns pod cpu mem _ _; do
		printf "%-25s %-45s %12s %14s\n" "$ns" "$pod" "$cpu" "$mem"
	done <<< "$sorted"
fi
