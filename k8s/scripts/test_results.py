import csv
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
import results


class ResultsTest(unittest.TestCase):
    def test_energy_summary_groups_sessions_by_task_and_node(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            energy_dir = run_dir / "energy"
            energy_dir.mkdir()

            with (energy_dir / "sessions.tsv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream, delimiter="\t")
                writer.writerow([
                    "node", "pid", "process_start", "pod_uid", "container_id", "metric",
                    "average_power_w", "total_energy_j", "status", "started_at", "ended_at", "task",
                ])
                writer.writerow(["node-a", "10", "100", "pod-a", "cid-a", "cpu", "4", "12.5", "ok", "1", "2", "random_walk.py"])
                writer.writerow(["node-a", "10", "100", "pod-a", "cid-a", "ram", "1", "2.5", "ok", "1", "2", "random_walk.py"])

            with (energy_dir / "agents.tsv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream, delimiter="\t")
                writer.writerow(["node", "status"])
                writer.writerow(["node-a", "completed"])

            results.energy_summary(run_dir)
            summary = json.loads((energy_dir / "summary.json").read_text(encoding="utf-8"))

            self.assertEqual(summary["total_energy_j"], 15.0)
            self.assertEqual(summary["by_node_j"], {"node-a": 15.0})
            self.assertEqual(summary["by_task_j"], {"random_walk.py": 15.0})
            self.assertEqual(summary["measurement_status"], "complete")
            self.assertEqual(summary["valid_session_count"], 2)

    def test_energy_summary_rejects_empty_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "no valid measurement"):
                results.energy_summary(run_dir)
            summary = json.loads((run_dir / "energy" / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["measurement_status"], "failed")

    def test_energy_summary_marks_agent_failure_partial(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            energy_dir = run_dir / "energy"
            energy_dir.mkdir()
            with (energy_dir / "sessions.tsv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream, delimiter="\t")
                writer.writerow(["node", "pid", "metric", "total_energy_j", "status", "task"])
                writer.writerow(["node-a", "10", "cpu", "5", "ok", "task.py"])
            with (energy_dir / "agents.tsv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream, delimiter="\t")
                writer.writerow(["node", "status"])
                writer.writerow(["node-a", "failed_after_ready"])

            with contextlib.redirect_stderr(io.StringIO()):
                results.energy_summary(run_dir)
            summary = json.loads((energy_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["measurement_status"], "partial")

    def test_archive_failure_does_not_replace_workload_status(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            args = SimpleNamespace(
                status="Succeeded",
                mode="embedding-training-inference-evaluation",
                archive_status="failed",
            )
            results.write_manifest(run_dir, args)
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "Succeeded")
            self.assertEqual(manifest["archive_status"], "failed")


if __name__ == "__main__":
    unittest.main()
