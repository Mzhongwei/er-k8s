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


if __name__ == "__main__":
    unittest.main()
