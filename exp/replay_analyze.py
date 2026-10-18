"""Time and RAPL/NVML energy of the replayed windows (output of exp/run_replay.sh).

  python3 exp/replay_analyze.py reports/replay-<dataset>-<time> [node]

For every repetition, the phases of exp/replay_stage.py are matched with the node's samples:
incremental (above-idle, baseline from the idle period before the replays) energy of CPU
package, DRAM and GPU per phase; on a virtual node, its attributed share of the host. If other
nodes were sampled (the NFS server when replaying on another node), their increment is added
as <phase>_storage_j. Writes results.csv and prints per (state, alternative) means:

  Word2Vec  restore (model load), train, write; train energy per window; reconfiguration
            latency L = Pod start (creation -> container start) + imports + restore
  BERT      judge time and energy per window
"""
from __future__ import annotations

import csv
import importlib.util
import json
import statistics
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("rapl", ROOT / "k8s/monitoring/rapl/rapl.py")
rapl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rapl)


def incremental(samples: Path, start: float, end: float, baselines) -> dict[str, dict[str, float]]:
    summary = rapl.summarize(samples, start, end, baselines)
    return {name: {component: node["incremental_j"].get(component, 0.0) for component in ("cpu", "ram", "gpu")}
            for name, node in summary["nodes"].items()}


def main(out: Path, node: str | None = None) -> int:
    samples = out / "rapl"
    nodes = list(json.loads((samples / "sampler.json").read_text())["nodes"])
    node = node or nodes[0]
    others = [name for name in nodes if name != node]
    marks = {label: float(t) for t, label in
             (line.split("\t", 1) for line in (samples / "marks.tsv").read_text().splitlines())}
    baselines = [(marks["idle-before-start"], marks["idle-before-end"])]
    pods = {}
    for line in (out / "pods.jsonl").read_text().splitlines():
        pod = json.loads(line)
        parse = lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()  # noqa: E731
        pods[pod["job"]] = parse(pod["started"]) - parse(pod["created"]) if pod.get("started") else None

    rows = []
    for line in (out / "replay.jsonl").read_text().splitlines():
        record = json.loads(line)
        if record["mode"] == "word2vec":
            job = f"replay-w2v-{record['state']}-n{record['workers']}"
            phases = {"restore": ("start", "loaded"), "train": ("loaded", "trained"), "write": ("trained", "written")}
            alternative = f"{record['workers']} workers"
        else:
            job = f"replay-bert-{record['device'].replace(':', '-')}"
            phases = {"restore": ("start", "loaded"), "judge": ("loaded", "judged")}
            alternative = record["device"]
        row = {"stage": record["mode"], "state": record["state"], "alternative": alternative, "rep": record["rep"],
               "pod_start_s": pods.get(job), "imports_s": record["imports_done"] - record["process_start"]}
        for phase, (a, b) in phases.items():
            row[f"{phase}_s"] = round(record[b] - record[a], 3)
            per_node = incremental(samples, record[a], record[b], baselines)
            energy = per_node[node]
            row[f"{phase}_j"] = round(sum(energy.values()), 1)
            if others:
                row[f"{phase}_storage_j"] = round(sum(sum(per_node[name].values()) for name in others), 1)
            row[f"{phase}_cpu_j"] = round(energy["cpu"], 1)
            row[f"{phase}_gpu_j"] = round(energy["gpu"], 1)
        if record["mode"] == "word2vec" and row["pod_start_s"] is not None:
            row["reconfiguration_s"] = round(row["pod_start_s"] + row["imports_s"] + row["restore_s"], 1)
        if record["mode"] == "bert":
            row["pairs"] = record["pairs"]
        rows.append(row)

    fields = sorted({key for row in rows for key in row}, key=lambda k: (k not in ("stage", "state", "alternative", "rep"), k))
    with (out / "results.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    def mean(group, key):
        values = [row[key] for row in group if row.get(key) is not None]
        return statistics.mean(values) if values else float("nan")

    groups: dict[tuple, list] = {}
    for row in rows:
        groups.setdefault((row["stage"], row["state"], row["alternative"]), []).append(row)
    print(f"{'stage':9s} {'state':6s} {'alternative':12s} {'restore s':>9s} {'work s':>8s} {'write s':>8s} "
          f"{'work J':>8s} {'L s':>6s}")
    for (stage, state, alternative), group in sorted(groups.items(), key=lambda item: (item[0][0], item[0][1], item[1][0].get("train_s", 0))):
        work = "train" if stage == "word2vec" else "judge"
        print(f"{stage:9s} {state:6s} {alternative:12s} {mean(group, 'restore_s'):9.1f} {mean(group, work + '_s'):8.1f} "
              f"{mean(group, 'write_s'):8.1f} {mean(group, work + '_j'):8.0f} {mean(group, 'reconfiguration_s'):6.1f}")
    print(f"\nresults: {out / 'results.csv'}  (energy on {node}: incremental CPU package + DRAM + GPU, "
          f"idle baseline before the replays" + (f"; storage side on {', '.join(others)} in *_storage_j)" if others else ")"))
    return 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]), sys.argv[2] if len(sys.argv) > 2 else None))
