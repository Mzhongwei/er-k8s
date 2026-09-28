from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
GRAPH_MODULES = (
    "models/representation_graph.py",
    "models/graph_backend.py",
    "models/compact_adjacency_graph.py",
)


class GraphImageContentsTests(unittest.TestCase):
    def test_graph_backends_are_packaged_in_every_graph_runtime_image(self):
        for dockerfile_name in ("Dockerfile.graph", "Dockerfile.embedding-training"):
            content = (ROOT / "docker" / dockerfile_name).read_text(encoding="utf-8")
            for module in GRAPH_MODULES:
                with self.subTest(dockerfile=dockerfile_name, module=module):
                    self.assertIn(
                        f"Energy-Aware-Entity-Resolution/{module}",
                        content,
                    )



class RuntimeStorageDeletionTests(unittest.TestCase):
    def test_runtime_pvcs_use_delete_storage_class(self):
        manifests = sorted((ROOT / "k8s/pvc-manifests").glob("*.yaml"))
        self.assertTrue(manifests)
        for manifest in manifests:
            with self.subTest(manifest=manifest.name):
                self.assertIn(
                    "storageClassName: nfs-client-delete",
                    manifest.read_text(encoding="utf-8"),
                )

    def test_delete_storage_class_and_pipeline_wiring(self):
        storage = (ROOT / "k8s/storage-classes/nfs-client-delete.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn("name: nfs-client-delete", storage)
        self.assertIn('archiveOnDelete: "false"', storage)
        pipeline = (ROOT / "k8s/pipeline/pipeline.sh").read_text(encoding="utf-8")
        self.assertIn("storage-classes/nfs-client-delete.yaml", pipeline)
        self.assertIn('kubectl wait --for=delete "$pvc_ref"', pipeline)

if __name__ == "__main__":
    unittest.main()
