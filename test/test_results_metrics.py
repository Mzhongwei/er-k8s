"""results.py: per-window wait/compute records and energy per compute second."""
import contextlib
import csv
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

MODULE = Path(__file__).resolve().parents[1] / "k8s/monitoring/results.py"
SPEC = importlib.util.spec_from_file_location("results", MODULE)
results = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(results)


def window(stage, index, wait, compute, start):
    return "[EAER_WINDOW_METRICS] " + json.dumps({
        "stage": stage, "window": index, "wait_seconds": wait, "compute_seconds": compute,
        "started_at": start, "ended_at": start,
    })


LOGS = {
    "graph-construction-abc": "\n".join([
        "[INFO] noise",
        window("graph_construction", "setup", 0.0, 2.0, "2026-01-01T00:00:00+00:00"),
        window("graph_construction", 1, 6.0, 2.0, "2026-01-01T00:00:02+00:00"),
        window("graph_construction", 2, 3.0, 1.0, "2026-01-01T00:00:10+00:00"),
        window("graph_construction", "eos", 1.0, 0.0, "2026-01-01T00:00:14+00:00"),
        '[EAER_STEP_METRICS] {"logical_read_bytes":1,"logical_write_bytes":2,"storage_read_bytes":3,'
        '"storage_write_bytes":4,"elapsed_seconds":15.0,"wait_seconds":10.0,"compute_seconds":5.0,"windows":2}',
    ]),
    # A one-shot Pod that never used the stage runner: no wait/compute split available.
    "berttrai-training-xyz": '[EAER_STEP_METRICS] {"logical_read_bytes":9,"logical_write_bytes":9,'
                             '"storage_read_bytes":9,"storage_write_bytes":9,"elapsed_seconds":30.0}',
}


def fake_kubectl(command, **kwargs):
    pod = command[4]
    return subprocess.CompletedProcess(command, 0, stdout=LOGS.get(pod, ""), stderr="")


class ResultsMetricsTests(unittest.TestCase):
    def make_run(self, directory: Path) -> Path:
        run = directory / "run"
        (run / "energy").mkdir(parents=True)
        rows = [
            {"phase": "incremental", "task": "graph-construction", "pod": "graph-construction-abc",
             "pod_uid": "u1", "node": "n1", "status": "Succeeded",
             "started_at": "2026-01-01T00:00:00Z", "finished_at": "2026-01-01T00:00:15Z"},
            {"phase": "batch", "task": "berttrai-training", "pod": "berttrai-training-xyz",
             "pod_uid": "u2", "node": "n2", "status": "Succeeded",
             "started_at": "2026-01-01T00:00:00Z", "finished_at": "2026-01-01T00:00:30Z"},
        ]
        with (run / "placement.tsv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=results.PLACEMENT_FIELDS, delimiter="\t")
            writer.writeheader()
            for row in rows:
                writer.writerow({field: row.get(field, "") for field in results.PLACEMENT_FIELDS})
        return run

    def collect(self, run: Path):
        with mock.patch.object(results.subprocess, "run", side_effect=fake_kubectl):
            results.collect_step_metrics(run, "argo")

    def test_step_and_window_metrics_are_collected(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))
            self.collect(run)
            steps = {r["pod"]: r for r in csv.DictReader(io.StringIO((run / "step-metrics.tsv").read_text()), delimiter="\t")}
            graph = steps["graph-construction-abc"]
            self.assertEqual((graph["wait_seconds"], graph["compute_seconds"], graph["windows"]), ("10.0", "5.0", "2"))
            self.assertEqual(steps["berttrai-training-xyz"]["compute_seconds"], "")
            windows = list(csv.DictReader(io.StringIO((run / "window-metrics.tsv").read_text()), delimiter="\t"))
            self.assertEqual([w["window"] for w in windows], ["setup", "1", "2", "eos"])
            self.assertEqual(sum(float(w["wait_seconds"]) for w in windows), 10.0)
            self.assertEqual(sum(float(w["compute_seconds"]) for w in windows), 5.0)
            summary = json.loads((run / "step-metrics-summary.json").read_text())["steps"]
            item = summary["incremental/graph-construction"]
            self.assertEqual((item["windows"], item["cumulative_wait_seconds"], item["cumulative_compute_seconds"]),
                             (2, 10.0, 5.0))

    def test_energy_is_normalized_by_compute_time_for_both_providers(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))
            self.collect(run)
            self.assertFalse((run / "energy/compute-normalized.json").exists())  # no energy yet
            (run / "energy/ecofloc-summary.json").write_text(json.dumps({
                "provider": "ecofloc", "by_pod_j": {"graph-construction-abc": 30.0, "berttrai-training-xyz": 99.0}}))
            (run / "energy/alumet-summary.json").write_text(json.dumps({
                "provider": "alumet", "by_workload_pod_j": {"graph-construction-abc": 45.0}}))
            results.write_compute_normalized(run)
            data = json.loads((run / "energy/compute-normalized.json").read_text())["providers"]
            eco = {p["pod"]: p for p in data["ecofloc"]["pods"]}
            graph = eco["graph-construction-abc"]
            # 5s compute of 15s active -> a third of the energy; 30 J / 5 compute-s = 6 J/s.
            self.assertAlmostEqual(graph["compute_fraction"], 1 / 3, places=5)
            self.assertAlmostEqual(graph["compute_energy_j"], 10.0)
            self.assertAlmostEqual(graph["energy_per_compute_second_j"], 6.0)
            # A Pod without window metrics is listed but never invents compute numbers or totals.
            self.assertIsNone(eco["berttrai-training-xyz"]["compute_seconds"])
            self.assertEqual(data["ecofloc"]["measured_pods"], 1)
            self.assertAlmostEqual(data["ecofloc"]["energy_j"], 30.0)
            self.assertAlmostEqual(data["alumet"]["pods"][0]["energy_per_compute_second_j"], 9.0)

    def test_show_prints_wait_compute_and_energy_rate(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = self.make_run(Path(tmp))
            self.collect(run)
            (run / "energy/ecofloc-summary.json").write_text(json.dumps({
                "provider": "ecofloc", "total_energy_j": 30.0, "by_pod_j": {"graph-construction-abc": 30.0}}))
            results.write_compute_normalized(run)
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                results.show(run)
            text = buffer.getvalue()
            self.assertIn("compute=5.000s wait=10.000s", text)
            self.assertIn("J/compute-s=6.000", text)


if __name__ == "__main__":
    unittest.main()
