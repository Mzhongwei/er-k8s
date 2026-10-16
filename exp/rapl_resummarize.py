"""Recompute the RAPL/NVML energy of a benchmark's runs from its samples.

Summaries written at the end of a run before the fix that waits for samples past the end could miss
the last sample and report CPU/DRAM as uncovered. This recomputes every run of an output
directory of exp/serie1-motivation.py --rapl from <output>/rapl/<batch>/, with the idle
baseline recorded before that batch (marks.tsv), rewrites <run_dir>/energy/rapl-summary.json
when the run directory exists, updates the rapl_* columns of runs.csv / offline-prep.csv, and
rewrites rapl-idle-drift.json.

  python3 exp/rapl_resummarize.py reports/baseline-single-s2 [more output dirs]
"""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


serie1 = load("serie1", ROOT / "exp/serie1-motivation.py")
rapl = load("rapl", ROOT / "k8s/monitoring/rapl/rapl.py")


def marks(batch: Path) -> dict[str, float]:
    found = {}
    path = batch / "marks.tsv"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            t, label = line.split("\t", 1)
            found[label] = float(t)
    return found


def batch_span(batch: Path) -> tuple[float, float]:
    rows = rapl.read_tsv(next(batch.glob("rapl-*.tsv")))
    return float(rows[0]["t"]), float(rows[-1]["t"])


def rapl_fields(summary: dict) -> dict:
    """The rapl_* columns, as exp/serie1-motivation.py extract_energy derives them."""
    with tempfile.TemporaryDirectory() as directory:
        energy = Path(directory) / "energy"
        energy.mkdir()
        (energy / "rapl-summary.json").write_text(json.dumps(summary), encoding="utf-8")
        return {key: value for key, value in serie1.extract_energy(Path(directory)).items() if key.startswith("rapl_")}


def main(outputs: list[str]) -> int:
    for raw in outputs:
        output = Path(raw).resolve()
        batches = sorted(path for path in (output / "rapl").glob("*") if path.is_dir())
        if not batches:
            print(f"{output}: no rapl/ samples")
            continue
        spans = {batch: batch_span(batch) for batch in batches}
        for name, fields in (("runs.csv", serie1.RUN_FIELDS), ("offline-prep.csv", serie1.PREP_FIELDS)):
            path = output / name
            rows = serie1.load_rows(path)
            changed = 0
            for row in rows:
                if not row.get("started_at") or not row.get("finished_at"):
                    continue
                start, end = rapl.parse_time(row["started_at"]), rapl.parse_time(row["finished_at"])
                batch = next((b for b, (first, last) in spans.items() if first <= start and end <= last), None)
                if batch is None:
                    continue
                labels = marks(batch)
                baselines = [(labels["idle-before-start"], labels["idle-before-end"])] \
                    if "idle-before-start" in labels and "idle-before-end" in labels else []
                summary = rapl.summarize(batch, start, end, baselines)
                run_dir = ROOT / row["run_dir"] if row.get("run_dir") else None
                if run_dir and run_dir.is_dir():
                    (run_dir / "energy").mkdir(exist_ok=True)
                    (run_dir / "energy" / "rapl-summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
                row.update(rapl_fields(summary))
                changed += 1
                print(f"{name}: {row.get('dataset')} {row.get('run_dir', '')[-30:]}: {summary['measurement_status']}, "
                      f"total {summary['total_energy_j']:.0f} J, incremental {summary['incremental_energy_j']} J")
            if changed:
                serie1.write_csv(path, fields, rows)
        for batch in batches:
            labels = marks(batch)
            if {"idle-before-start", "idle-before-end", "idle-after-start", "idle-after-end"} <= set(labels):
                drift = rapl.summarize(batch, labels["idle-after-start"], labels["idle-after-end"],
                                       [(labels["idle-before-start"], labels["idle-before-end"])])
                (output / "rapl-idle-drift.json").write_text(json.dumps(drift, indent=2), encoding="utf-8")
                print(f"idle drift ({batch.name}): {drift['incremental_by_component_j']} J over "
                      f"{labels['idle-after-end'] - labels['idle-after-start']:.0f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or sys.exit(__doc__)))
