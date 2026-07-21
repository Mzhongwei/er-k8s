#!/usr/bin/env python3
from __future__ import annotations

import unittest

from compiler import apply_rule_to_kubernetes_job
from move import owner_job, pin_job
from scheduler import compile_policy_config, rank_nodes


class SchedulerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.nodes = {
            "cpu": {
                "cpu_class": "medium", "memory_class": "medium", "gpu_type": "none",
                "io_class": "medium", "workload_mode": "service", "storage_access": ["nfs"],
                "historical_energy": "low", "power_watts": 80,
                "cpu_utilization": 20, "memory_utilization": 30,
            },
            "gpu": {
                "cpu_class": "high", "memory_class": "high", "gpu_type": "T4",
                "io_class": "high", "workload_mode": "batch", "storage_access": ["ceph"],
                "historical_energy": "high", "power_watts": 250,
                "cpu_utilization": 80, "memory_utilization": 70,
            },
        }
        self.data = {
            "d": {"storage_class": "nfs", "size_class": "GB", "locations": ["cpu"]}
        }

    def best(self, strategy: str, **task: object) -> str:
        configured = {"task_id": "t", **task}
        return next(item.node for item in rank_nodes(configured, self.nodes, self.data, strategy) if item.eligible)

    def test_every_strategy(self) -> None:
        cases = {
            "B0": ({}, "cpu"),
            "C1": ({"gpu_required": True}, "gpu"),
            "C2": ({"workload_mode": "batch"}, "gpu"),
            "C3": ({"cpu_request": "high", "memory_request": "high"}, "gpu"),
            "C4": ({"data_ids": ["d"]}, "cpu"),
            "C5": ({"data_ids": ["d"]}, "cpu"),
            "C6": ({"deadline_class": "strict"}, "gpu"),
            "C7": ({}, "cpu"),
            "H1": ({}, "cpu"),
            "H2": ({}, "cpu"),
        }
        for strategy, (task, expected) in cases.items():
            with self.subTest(strategy=strategy):
                self.assertEqual(self.best(strategy, **task), expected)

    def test_composition_and_weights(self) -> None:
        ranked = rank_nodes(
            {"task_id": "t", "deadline_class": "strict"}, self.nodes, self.data,
            ["C6", "C7"], {"weights": {"C6": 1, "C7": 3}},
        )
        self.assertEqual(ranked[0].node, "cpu")

    def test_compile_policy_to_affinity_rule(self) -> None:
        config = {
            "version": 2, "strategy": "C1", "nodes": self.nodes,
            "incremental": {"templates": {"worker": {"task": {"gpu_required": True}}}},
        }
        rule = compile_policy_config(config)["incremental"]["worker"]
        self.assertEqual(rule["require"], ["gpu"])
        self.assertEqual(rule["prefer"], ["gpu"])
        self.assertEqual(rule["tags"], ["gpu"])

    def test_baseline_keeps_gpu_requirement_without_affinity(self) -> None:
        config = {
            "version": 2, "strategy": "B0",
            "batch": {"templates": {"gpu-task": {"task": {"gpu_required": True}}}},
        }
        rule = compile_policy_config(config)["batch"]["gpu-task"]
        self.assertEqual(rule["require"], [])
        self.assertEqual(rule["prefer"], [])
        self.assertEqual(rule["tags"], ["gpu"])

    def test_no_eligible_node_is_an_error(self) -> None:
        config = {
            "version": 2, "strategy": "C3", "nodes": {"small": {
                "cpu_class": "low", "memory_class": "low", "gpu_type": "none"
            }},
            "batch": {"templates": {"large": {"task": {"cpu_request": "high"}}}},
        }
        with self.assertRaisesRegex(ValueError, "No eligible node"):
            compile_policy_config(config)

    def test_rule_is_rendered_as_required_and_preferred_affinity(self) -> None:
        job = {"kind": "Job", "metadata": {"name": "worker"}, "spec": {
            "template": {"spec": {"containers": [{"name": "main"}]}}
        }}
        rule = {
            "require": ["gpu"], "prefer": ["gpu"], "fallback": [], "avoid": [],
            "tags": ["gpu"],
        }
        apply_rule_to_kubernetes_job(job, rule)
        pod = job["spec"]["template"]["spec"]
        affinity = pod["affinity"]["nodeAffinity"]
        self.assertIn("requiredDuringSchedulingIgnoredDuringExecution", affinity)
        self.assertIn("preferredDuringSchedulingIgnoredDuringExecution", affinity)
        self.assertEqual(pod["runtimeClassName"], "nvidia")
        self.assertEqual(pod["tolerations"][0]["key"], "nvidia.com/gpu")
        self.assertEqual(pod["containers"][0]["resources"]["limits"]["nvidia.com/gpu"], 1)

    def test_hot_move_pins_job_without_dropping_other_affinity(self) -> None:
        job = {"spec": {"template": {"spec": {"affinity": {"podAffinity": {}}}}}}
        pin_job(job, "cpu")
        affinity = job["spec"]["template"]["spec"]["affinity"]
        self.assertIn("podAffinity", affinity)
        terms = affinity["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
        self.assertEqual(terms["nodeSelectorTerms"][0]["matchFields"][0]["values"], ["cpu"])
        pod = {"metadata": {"ownerReferences": [{"kind": "Job", "name": "worker"}]}}
        self.assertEqual(owner_job(pod), "worker")


if __name__ == "__main__":
    unittest.main()
