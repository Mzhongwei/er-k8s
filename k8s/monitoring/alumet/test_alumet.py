import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("alumet.py")
SPEC = importlib.util.spec_from_file_location("eaer_alumet", MODULE_PATH)
alumet = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(alumet)


class AlumetSummaryTests(unittest.TestCase):
    def test_enable_collection_keeps_gpu_collector_on_labelled_nodes(self):
        calls = []

        def fake_kubectl(*args, **_kwargs):
            calls.append(args)
            return ""

        with (
            mock.patch.object(
                alumet, "resource_names",
                return_value=["eaer-alumet-alumet-relay-client", "eaer-alumet-gpu-alumet-relay-client"],
            ),
            mock.patch.object(alumet, "resource_name", side_effect=["relay-server", "influxdb"]),
            mock.patch.object(alumet, "eligible_nodes", return_value=["cpu-1", "gpu-1"]),
            mock.patch.object(alumet, "set_retention"),
            mock.patch.object(alumet, "kubectl", side_effect=fake_kubectl),
        ):
            alumet.enable_collection()

        patches = [call for call in calls if call[:2] == ("patch", "daemonset")]
        self.assertEqual(len(patches), 2)
        cpu_selector = json.loads(patches[0][patches[0].index("-p") + 1])
        gpu_selector = json.loads(patches[1][patches[1].index("-p") + 1])
        self.assertEqual(
            cpu_selector["spec"]["template"]["spec"]["nodeSelector"],
            {alumet.ENABLE_LABEL: "true"},
        )
        self.assertEqual(
            gpu_selector["spec"]["template"]["spec"]["nodeSelector"],
            {alumet.ENABLE_LABEL: "true", alumet.GPU_ENABLE_LABEL: "true"},
        )

    def test_rapl_components_avoid_overlapping_domains(self):
        self.assertEqual(
            alumet.rapl_components({"package_total": 10.0, "dram_total": 2.0, "core": 7.0}),
            {"cpu": 10.0, "dram": 2.0},
        )
        self.assertEqual(
            alumet.rapl_components({"platform_total": 15.0, "package_total": 10.0}),
            {"platform": 15.0},
        )

    def test_summary_breaks_attributed_energy_down_by_stage_and_hardware(self):
        with tempfile.TemporaryDirectory() as directory:
            run_dir = Path(directory)
            with (run_dir / "placement.tsv").open("w", encoding="utf-8", newline="") as output:
                writer = csv.DictWriter(
                    output, fieldnames=("phase", "task", "pod", "node"), delimiter="\t"
                )
                writer.writeheader()
                writer.writerow({
                    "phase": "training", "task": "random-walk", "pod": "rw-pod", "node": "gpu-1"
                })
            energy_dir = run_dir / "energy"
            energy_dir.mkdir()
            (energy_dir / "alumet-window.json").write_text(
                json.dumps({"ready_clients": [{"pod": "collector", "node": "gpu-1"}]}),
                encoding="utf-8",
            )
            rows = [
                {"_measurement": "rapl_consumed_energy_uj", "_field": "value",
                 "_value": "100000000", "resource_consumer_kind": "local_machine",
                 "node": "gpu-1", "domain": "package_total"},
                {"_measurement": "rapl_consumed_energy_uj", "_field": "value",
                 "_value": "20000000", "resource_consumer_kind": "local_machine",
                 "node": "gpu-1", "domain": "dram_total"},
                {"_measurement": "nvml_energy_consumption_mj", "_field": "value",
                 "_value": "30000", "resource_consumer_kind": "local_machine",
                 "resource_kind": "gpu", "node": "gpu-1"},
                {"_measurement": "attributed_rapl_energy", "_field": "value",
                 "_value": "40", "resource_consumer_kind": "cgroup", "name": "rw-pod"},
                {"_measurement": "attributed_nvml_energy", "_field": "value",
                 "_value": "12", "resource_consumer_kind": "cgroup", "name": "rw-pod"},
            ]

            with mock.patch.object(alumet, "influx_rows", return_value=iter(rows)):
                self.assertTrue(alumet.summarize(run_dir, "unused"))

            summary = json.loads((energy_dir / "alumet-summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["hardware_by_device_j"], {"cpu": 100.0, "dram": 20.0, "gpu": 30.0})
            self.assertEqual(summary["hardware_energy_j"], 150.0)
            self.assertEqual(
                summary["by_stage_hardware_j"]["training/random-walk"],
                {"cpu": 40.0, "gpu": 12.0},
            )
            self.assertEqual(summary["by_stage_j"]["training/random-walk"], 52.0)


if __name__ == "__main__":
    unittest.main()
