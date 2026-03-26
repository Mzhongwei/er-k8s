import os

graph_path = "./pipeline/graph/representation_graph.txt"
os.makedirs(os.path.dirname(graph_path), exist_ok=True)
with open(graph_path, "w", encoding="utf-8") as f:
    f.write("representation_graph_content")