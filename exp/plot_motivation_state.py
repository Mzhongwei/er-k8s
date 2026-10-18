"""Motivation figures for state growth (Observation 4) and overload (Observation 5).

  python3 exp/plot_motivation_state.py [--round5 DIR] [--round6 DIR] [--out DIR]

Inputs (defaults under experiment_records/rtas_data):
  round5 run directory: window-metrics.tsv of the retrieval-extrait-hn run (decision off,
    BERT on the P2200, 8 Word2Vec workers, 6,750-record windows every 135 s);
  round6 report: RAPL/NVML samples (rapl/<batch>/) and runs.csv of the same configuration.
Outputs PDFs for the paper and prints the numbers quoted in the text.
"""
from __future__ import annotations

import argparse
import csv
import statistics
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RECORDS = Path("/home/zhongwei/Data_integration/experiment_records/rtas_data")
ROUND5 = RECORDS / "round5/results/embedding-training-inference-evaluation-20261014-081018-1458128"
ROUND6 = RECORDS / "round6/baseline-single-s2"
# Word2Vec workers cycled over windows (2, 4, 8, 12 by window ordinal mod 4), RB_2 candidates.
ROUND9 = RECORDS / "round9/results/embedding-training-inference-evaluation-20261016-181537-3912421"
WORKERS_SCHEDULE = (2, 4, 8, 12)
# Ordinal blue ramp (steps 250/400/550/700), validated with validate_palette.js --ordinal.
WORKER_COLORS = ("#86b6ef", "#3987e5", "#1c5cab", "#0d366b")
PAPER = Path("/home/zhongwei/Documents/Papers-latex/RTAS_2027__ddl_05_11_2026__/figures")
WINDOW_RECORDS = 6750
PERIOD_S = 135.0  # 6,750 records x 20 ms

# Categorical slots in fixed order (validated: scripts/validate_palette.js, light mode). Aqua
# and yellow are below 3:1 on white: every series is direct-labelled and has its own dash.
STAGES = {  # task -> (label, color, dash)
    "embedding-training": ("Word2Vec training", "#2a78d6", "-"),
    "bert-matching": ("BERT matching", "#eb6834", "-"),
    "graph-construction": ("graph construction", "#1baf7a", (0, (5, 1.5))),
    "calculating-similarity": ("similarity", "#eda100", (0, (1.5, 1.2))),
}
INK, MUTED, GRID = "#0b0b0b", "#52514e", "#e4e3df"

plt.rcParams.update({
    "font.family": "serif", "font.serif": ["Times New Roman", "Times", "STIXGeneral", "DejaVu Serif"],
    "mathtext.fontset": "stix", "font.size": 8, "axes.labelsize": 8, "xtick.labelsize": 7,
    "ytick.labelsize": 7, "axes.edgecolor": MUTED, "axes.labelcolor": INK, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.5, "axes.axisbelow": True,
    "lines.linewidth": 1.2, "pdf.fonttype": 42, "savefig.bbox": "tight", "savefig.pad_inches": 0.02,
})
WIDTH, HEIGHT = 3.45, 1.75  # one IEEE column per panel


def rolling_median(values: list[float], width: int = 7) -> list[float]:
    half = width // 2
    return [statistics.median(values[max(0, i - half):i + half + 1]) for i in range(len(values))]


def window_metrics(run: Path) -> dict[str, dict[int, dict]]:
    by_task: dict[str, dict[int, dict]] = {}
    with (run / "window-metrics.tsv").open(newline="") as stream:
        for row in csv.DictReader(stream, delimiter="\t"):
            if row["window"].isdigit():
                by_task.setdefault(row["task"], {})[int(row["window"]) // WINDOW_RECORDS] = row
    return by_task


def label_end(ax, x, y, text, color, dy=0.0):
    ax.annotate(text, (x, y), xytext=(3, dy), textcoords="offset points", fontsize=7,
                color=INK, va="center", ha="left")
    ax.plot([x], [y], marker="o", markersize=2.5, color=color, zorder=3)


def figure_growth(metrics, out: Path) -> None:
    # (a) per-window busy time
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    offsets = {"bert-matching": 0, "embedding-training": 0, "graph-construction": 2, "calculating-similarity": -3}
    for task, (label, color, dash) in STAGES.items():
        windows = sorted(metrics[task])
        busy = rolling_median([float(metrics[task][w]["compute_seconds"]) for w in windows])
        ax.plot(windows, busy, color=color, linestyle=dash)
        label_end(ax, windows[-1], busy[-1], label, color, offsets[task])
    ax.axhline(PERIOD_S, color=MUTED, linewidth=0.8, linestyle=(0, (4, 2)))
    ax.annotate(f"window period $T$ = {PERIOD_S:.0f} s", (2, PERIOD_S), xytext=(0, 3),
                textcoords="offset points", fontsize=7, color=MUTED)
    ax.set_xlabel("window index (6,750 records each)")
    ax.set_ylabel("busy time per window (s)")
    ax.set_ylim(0, 360)
    ax.set_xlim(0, 215)  # room for the direct labels inside the axes
    ax.set_xticks(range(0, 151, 25))
    fig.savefig(out / "motiv_d_growth_time.pdf")
    plt.close(fig)

    # (b) state written per window
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    for task, text in (("embedding-training", "Word2Vec checkpoint"), ("graph-construction", "graph snapshot")):
        label, color, dash = STAGES[task]
        windows = sorted(metrics[task])
        size = rolling_median([int(metrics[task][w]["transfer_write_bytes"]) / 1e9 for w in windows])
        ax.plot(windows, size, color=color, linestyle=dash)
        label_end(ax, windows[-1], size[-1], text, color)
    ax.set_xlabel("window index (6,750 records each)")
    ax.set_ylabel("state written per window (GB)")
    ax.set_ylim(0, None)
    ax.set_xlim(0, 215)  # room for the direct labels inside the axes
    ax.set_xticks(range(0, 151, 25))
    fig.savefig(out / "motiv_e_growth_state.pdf")
    plt.close(fig)


def stats_growth(metrics) -> None:
    def segment(task, field, lo, hi, scale=1.0):
        windows = sorted(metrics[task])
        part = windows[int(lo * len(windows)):int(hi * len(windows))]
        return statistics.mean(float(metrics[task][w][field]) for w in part) * scale
    for task in ("graph-construction", "embedding-training", "calculating-similarity", "random-walk", "bert-matching"):
        print(f"{task:24s} busy {segment(task, 'compute_seconds', 0, .1):6.1f} -> {segment(task, 'compute_seconds', .9, 1):6.1f} s"
              f"  transfer {segment(task, 'transfer_seconds', 0, .1):5.1f} -> {segment(task, 'transfer_seconds', .9, 1):5.1f} s"
              f"  write {segment(task, 'transfer_write_bytes', 0, .1, 1e-9):5.2f} -> {segment(task, 'transfer_write_bytes', .9, 1, 1e-9):5.2f} GB")
    xs = [int(metrics["embedding-training"][w]["transfer_write_bytes"]) for w in sorted(metrics["embedding-training"])]
    ys = [float(metrics["embedding-training"][w]["compute_seconds"]) for w in sorted(metrics["embedding-training"])]
    print(f"corr(Word2Vec busy time, checkpoint bytes) = {statistics.correlation(xs, ys):.2f}")


def figure_backlog(metrics, out: Path) -> None:
    """Completion lag: when a stage finished window k, minus when window k's ingestion started."""
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    start = {w: datetime.fromisoformat(r["started_at"]) for w, r in metrics["normalization"].items()}
    for task, dy in (("bert-matching", 0), ("embedding-training", 4)):
        label, color, dash = STAGES[task]
        windows = sorted(w for w in metrics[task] if w in start)
        lag = [(datetime.fromisoformat(metrics[task][w]["ended_at"]) - start[w]).total_seconds() / 3600 for w in windows]
        ax.plot(windows, lag, color=color, linestyle=dash)
        label_end(ax, windows[-1], lag[-1], f"{label}: {lag[-1]:.1f} h", color, dy)
        print(f"{task:24s} lag first {lag[0] * 60:.1f} min, last {lag[-1]:.2f} h")
    ax.set_xlabel("window index (6,750 records each)")
    ax.set_ylabel("completion lag after arrival (h)")
    ax.set_ylim(0, None)
    ax.set_xlim(0, 215)  # room for the direct labels inside the axes
    ax.set_xticks(range(0, 151, 25))
    fig.savefig(out / "motiv_f_backlog.pdf")
    plt.close(fig)


def read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream, delimiter="\t"))


def figure_power(report: Path, out: Path) -> None:
    """Node power over the retrieval run, 5-minute means, from the RAPL/NVML samples."""
    batch = next(path for path in (report / "rapl").iterdir() if path.is_dir())
    run = next(r for r in csv.DictReader((report / "runs.csv").open()) if r["dataset"] == "retrieval-extrait-hn")
    t0, t1 = (datetime.fromisoformat(run[k]).timestamp() for k in ("started_at", "finished_at"))
    bins = 300.0
    rapl: dict[str, list[tuple[float, float]]] = {}
    for row in read_tsv(batch / "rapl-server2-labo.tsv"):
        rapl.setdefault(row["name"].split("-")[0], []).append((float(row["t"]), float(row["energy_uj"])))
    series = {}
    for name, samples in rapl.items():  # counter deltas per bin (no wrap within 300 s at these powers)
        power: dict[int, list[float]] = {}
        for (ta, ea), (tb, eb) in zip(samples, samples[1:]):
            if t0 <= ta < t1 and eb >= ea:
                power.setdefault(int((ta - t0) // bins), []).append((eb - ea) / 1e6 / (tb - ta))
        series[name] = power
    gpu: dict[int, list[float]] = {}
    for row in read_tsv(batch / "gpu-server2-labo.tsv"):
        t = float(row["t"])
        if t0 <= t < t1:
            gpu.setdefault(int((t - t0) // bins), []).append(float(row["power_w"]))
    series["gpu"] = gpu

    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    style = {"package": ("CPU package", STAGES["embedding-training"][1], "-"),
             "gpu": ("GPU (P2200)", STAGES["bert-matching"][1], "-"),
             "dram": ("DRAM", MUTED, (0, (4, 1.5)))}
    curves = {}
    for name, (label, color, dash) in style.items():
        keys = sorted(series[name])
        hours = [(k + 0.5) * bins / 3600 for k in keys]
        watts = [statistics.mean(series[name][k]) for k in keys]
        curves[name] = (hours, watts)
        ax.plot(hours, watts, color=color, linestyle=dash)
    # Upstream stages done: the CPU package drops for good (first 5-minute bin below 52 W after 1 h).
    hours, watts = curves["package"]
    done = next(h for h, w in zip(hours, watts) if h > 1 and w < 52)
    ax.axvline(done, color=MUTED, linewidth=0.8, linestyle=(0, (1, 1.5)))
    ax.annotate(f"upstream stages done ({done:.1f} h)", (done, 78), xytext=(4, 0), textcoords="offset points",
                fontsize=7, color=MUTED, va="top", ha="left")
    # Labels inside the plateau (the run's last bin is the shutdown drop).
    for name, dy in (("package", 5), ("gpu", -6), ("dram", 5)):
        hours, watts = curves[name]
        x = 9.5
        y = statistics.mean(w for h, w in zip(hours, watts) if 9 < h < 10)
        ax.annotate(style[name][0], (x, y), xytext=(0, dy), textcoords="offset points", fontsize=7,
                    color=INK, ha="center", va="bottom" if dy > 0 else "top")
        early = [w for h, w in zip(hours, watts) if 0.5 < h < done - 0.2]
        late = [w for h, w in zip(hours, watts) if done + 0.5 < h < hours[-1] - 0.3]
        print(f"{name:8s} mean power before {done:.1f} h {statistics.mean(early):5.1f} W, after {statistics.mean(late):5.1f} W")
    ax.set_xlabel("time since the stream started (h)")
    ax.set_ylabel("power (W)")
    ax.set_ylim(0, 80)
    fig.savefig(out / "motiv_g_power.pdf")
    plt.close(fig)
    print(f"run duration {(t1 - t0) / 3600:.2f} h")


def figure_workers(metrics, out: Path) -> None:
    """(a) Word2Vec busy time per window for each worker count of the schedule."""
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    windows_all = metrics["embedding-training"]
    ends = {}
    for k, (workers, color) in enumerate(zip(WORKERS_SCHEDULE, WORKER_COLORS)):
        windows = [w for w in sorted(windows_all) if w % len(WORKERS_SCHEDULE) == k]
        busy = rolling_median([float(windows_all[w]["compute_seconds"]) for w in windows], width=5)
        ax.plot(windows, busy, color=color)
        ends[workers] = (windows[-1], busy[-1], color)
        print(f"{workers:2d} workers: first {busy[0]:6.1f} s, last {busy[-1]:6.1f} s")
    offsets = {2: 0, 4: 2, 8: -6, 12: 6}  # 8 and 12 end close together
    for workers, (x, y, color) in ends.items():
        label_end(ax, x, y, f"{workers} workers", color, offsets[workers])
    ax.axhline(PERIOD_S, color=MUTED, linewidth=0.8, linestyle=(0, (4, 2)))
    # The lines leave no room for a label on the plot: T is marked on the axis instead.
    ax.set_yticks([0, 50, 100, PERIOD_S, 200, 250, 300])
    ax.set_yticklabels(["0", "50", "100", f"$T$={PERIOD_S:.0f}", "200", "250", "300"])
    ax.set_xlabel("window index (6,750 records each)")
    ax.set_ylabel("Word2Vec busy time per window (s)")
    ax.set_ylim(0, 300)
    ax.set_xlim(0, 195)
    ax.set_xticks(range(0, 151, 25))
    fig.savefig(out / "motiv_h_workers.pdf")
    plt.close(fig)


def figure_shift(metrics, out: Path) -> None:
    """(b) Demand shifts between stages: BERT falls while graph construction and the
    Word2Vec checkpoint handover grow."""
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    series = (
        ("bert-matching", "compute_seconds", "BERT matching", STAGES["bert-matching"][1], "-", 0),
        ("graph-construction", "compute_seconds", "graph construction", STAGES["graph-construction"][1],
         STAGES["graph-construction"][2], 3),
        ("embedding-training", "transfer_seconds", "Word2Vec checkpoint write", STAGES["embedding-training"][1],
         (0, (1.5, 1.2)), -4),
    )
    for task, field, label, color, dash, dy in series:
        windows = sorted(metrics[task])
        values = rolling_median([float(metrics[task][w][field]) for w in windows])
        ax.plot(windows, values, color=color, linestyle=dash)
        label_end(ax, windows[-1], values[-1], label, color, dy)
        print(f"{label}: first {values[0]:6.1f} s, last {values[-1]:6.1f} s")
    ax.set_xlabel("window index (6,750 records each)")
    ax.set_ylabel("time per window (s)")
    ax.set_ylim(0, 160)
    ax.set_xlim(0, 215)
    ax.set_xticks(range(0, 151, 25))
    fig.savefig(out / "motiv_i_shift.pdf")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--round5", type=Path, default=ROUND5)
    parser.add_argument("--round6", type=Path, default=ROUND6)
    parser.add_argument("--round9", type=Path, default=ROUND9)
    parser.add_argument("--out", type=Path, default=PAPER)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    metrics = window_metrics(args.round5)
    figure_growth(metrics, args.out)
    stats_growth(metrics)
    figure_backlog(metrics, args.out)
    figure_power(args.round6, args.out)
    round9 = window_metrics(args.round9)
    figure_workers(round9, args.out)
    figure_shift(round9, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
