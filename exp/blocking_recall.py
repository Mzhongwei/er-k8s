"""Where does the embedding candidate generation lose true matches?

For every ground-truth pair (A, B), with a run's final embedding model, computes
  - the rank of A among all A records by cosine similarity to B (what the B side of the
    pipeline's mutual top-k ranks: every window is a cross block of all A x the window's B),
  - the rank of B among the B records of its own window (the A side of the mutual top-k),
and reports recall@k for each side, for both (mutual: what the pipeline keeps) and for
either side. The pipeline scored every window with that window's checkpoint, not the final
model, so the figures approximate the run's candidate recall.

Usage (inside the embedding image, see exp/run_blocking_recall.sh):
  python blocking_recall.py --model /app/data/embedding/<version>/... --table-a A.csv
      --table-b B.csv --matches matches.txt --window-count 6750 --output DIR
Outputs DIR/report.json and DIR/ranks.csv (left_id, right_id, window, rank_b_to_a, rank_a_to_b).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, "/app")
from models.embedding_model import EmbeddingModel  # noqa: E402

KS = (1, 2, 3, 5, 10, 20, 50, 100, 200)
csv.field_size_limit(1 << 30)


def resolve_model(path: str) -> str:
    """A model file, or a directory holding the 'current' checkpoint reference."""
    if os.path.isdir(path):
        reference = os.path.join(path, "current.json")
        with open(reference, encoding="utf-8") as stream:
            payload = json.load(stream)
        return (payload.get("value") or payload)["checkpoint_path"]
    return path


def read_ids(path: str, field: str, prefix: str) -> list[str]:
    with open(path, newline="", encoding="utf-8", errors="replace") as stream:
        reader = csv.DictReader(stream)
        return [value if value.startswith(prefix) else prefix + value
                for value in (row[field].strip() for row in reader)]


def read_matches(path: str, a_prefix: str, b_prefix: str) -> list[tuple[str, str]]:
    pairs = []
    with open(path, newline="", encoding="utf-8") as stream:
        for row in csv.reader(stream):
            if len(row) < 2 or row[0].strip().lower() in {"left_id", "ltable_id", "id1"}:
                continue
            left, right = row[0].strip(), row[1].strip()
            if left.startswith(b_prefix) or right.startswith(a_prefix):
                left, right = right, left
            pairs.append((left if left.startswith(a_prefix) else a_prefix + left,
                          right if right.startswith(b_prefix) else b_prefix + right))
    return pairs


def unit_rows(kv, ids: list[str]) -> np.ndarray:
    vectors = np.asarray([kv[i] for i in ids], dtype=np.float32)
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return vectors / norms


def recall_table(ranks: np.ndarray, total: int) -> dict[str, float]:
    return {str(k): round(float((ranks <= k).sum()) / total, 4) for k in KS}


PAIR_KS = (1, 3, 5, 10, 20)


def count_pairs(kv, a_units, a_known, b_ids, usable, window_of, args) -> dict:
    """Candidate pairs per window and rule (b_to_a, a_to_b, mutual, either) at each k of
    PAIR_KS, scoring all A against the B records of evenly spaced windows: the BERT load a
    rule would generate, next to the share of those pairs that are true matches."""
    top = max(PAIR_KS)
    window_total = (len(b_ids) + args.window_count - 1) // args.window_count
    sampled = sorted({int(i) for i in np.linspace(0, window_total - 1, min(args.pair_windows, window_total))})
    a_row = {a: i for i, a in enumerate(a_known)}
    sums = {rule: {k: [0, 0] for k in PAIR_KS} for rule in ("b_to_a", "a_to_b", "mutual", "either")}
    true_total = 0
    for window in sampled:
        members = [b for b in b_ids[window * args.window_count:(window + 1) * args.window_count] if b in kv]
        if len(members) < top:
            continue
        b_units = unit_rows(kv, members)
        col = {b: i for i, b in enumerate(members)}
        row_top = np.empty((len(a_known), top), dtype=np.int64)       # A side: best B columns
        row_score = np.empty((len(a_known), top), dtype=np.float32)
        col_best = np.full((top, len(members)), -np.inf, dtype=np.float32)  # B side: best scores
        for start in range(0, len(a_known), 8192):
            scores = a_units[start:start + 8192] @ b_units.T
            part = np.argpartition(-scores, top - 1, axis=1)[:, :top]
            part_scores = np.take_along_axis(scores, part, axis=1)
            order = np.argsort(-part_scores, axis=1)
            row_top[start:start + len(scores)] = np.take_along_axis(part, order, axis=1)
            row_score[start:start + len(scores)] = np.take_along_axis(part_scores, order, axis=1)
            merged = np.concatenate([col_best, np.partition(-scores, top - 1, axis=0)[:top] * -1])
            col_best = -np.partition(-merged, top - 1, axis=0)[:top]
        col_sorted = -np.sort(-col_best, axis=0)  # row k-1: the k-th best A score of each B
        truth = [(a_row[a], col[b]) for a, b in usable if window_of[b] == window and b in col]
        true_total += len(truth)
        true_rows = np.array([r for r, _ in truth], dtype=np.int64)
        true_cols = np.array([c for _, c in truth], dtype=np.int64)
        true_scores = np.sum(a_units[true_rows] * b_units[true_cols], axis=1) if truth else np.zeros(0)
        for k in PAIR_KS:
            threshold = col_sorted[k - 1]
            mutual = int((row_score[:, :k] >= threshold[row_top[:, :k]]).sum())
            b_pairs, a_pairs = len(members) * k, len(a_known) * k
            # The true pair's score is recomputed apart from the matrix product: allow for
            # float32 rounding (cosines of unit vectors, d terms).
            b_true = (true_scores >= threshold[true_cols] - 1e-5) if truth else np.zeros(0, dtype=bool)
            a_true = (row_top[true_rows, :k] == true_cols[:, None]).any(axis=1) if truth else np.zeros(0, dtype=bool)
            for rule, pairs, hits in (
                ("b_to_a", b_pairs, int(b_true.sum())),
                ("a_to_b", a_pairs, int(a_true.sum())),
                ("mutual", mutual, int((b_true & a_true).sum())),
                ("either", b_pairs + a_pairs - mutual, int((b_true | a_true).sum())),
            ):
                sums[rule][k][0] += pairs
                sums[rule][k][1] += hits
        print(f"  pairs: window {window} done", flush=True)
    n = len(sampled)
    return {
        "windows": n, "sampled_windows": sampled, "k": [str(k) for k in PAIR_KS],
        "true_pairs_per_window": round(true_total / n, 1),
        **{rule: {str(k): {"pairs": pairs / n, "true_share": round(hits / pairs, 4) if pairs else 0.0,
                           "recall_in_sample": round(hits / true_total, 4) if true_total else 0.0}
                  for k, (pairs, hits) in by_k.items()}
           for rule, by_k in sums.items()},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--model", required=True, help="embedding checkpoint, or its directory with current.json")
    parser.add_argument("--table-a", required=True)
    parser.add_argument("--table-b", required=True)
    parser.add_argument("--matches", required=True)
    parser.add_argument("--window-count", type=int, required=True, help="records per window (kafka.window_count)")
    parser.add_argument("--source-field", default="id")
    parser.add_argument("--left-prefix", default="A_")
    parser.add_argument("--right-prefix", default="B_")
    parser.add_argument("--chunk", type=int, default=1024, help="B records scored per matrix product")
    parser.add_argument("--pair-windows", type=int, default=10,
                        help="windows (evenly spaced) in which to count candidate pairs per rule; 0: skip")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)
    started = time.time()

    model_path = resolve_model(args.model)
    print(f"model {model_path}", flush=True)
    kv = EmbeddingModel.load(model_path, mmap="r").wv
    a_ids = read_ids(args.table_a, args.source_field, args.left_prefix)
    b_ids = read_ids(args.table_b, args.source_field, args.right_prefix)
    truth = read_matches(args.matches, args.left_prefix, args.right_prefix)
    print(f"A={len(a_ids)} B={len(b_ids)} truth={len(truth)} ({time.time() - started:.0f}s)", flush=True)

    # The windows follow table_B's row order, as the producer streams it.
    window_of = {b: i // args.window_count for i, b in enumerate(b_ids)}
    a_known = [a for a in a_ids if a in kv]
    a_row = {a: i for i, a in enumerate(a_known)}
    usable = [(a, b) for a, b in truth if a in a_row and b in kv and b in window_of]
    missing = {
        "a_without_vector": len(a_ids) - len(a_known),
        "truth_pairs_unusable": len(truth) - len(usable),
        "truth_b_not_streamed": sum(1 for _, b in truth if b not in window_of),
    }
    print(f"usable truth pairs {len(usable)}; {missing}", flush=True)
    a_units = unit_rows(kv, a_known)

    # B side: rank of the true A among all A records.
    rank_b_to_a = np.empty(len(usable), dtype=np.int64)
    by_b: dict[str, list[int]] = {}
    for index, (_, b) in enumerate(usable):
        by_b.setdefault(b, []).append(index)
    b_list = list(by_b)
    for start in range(0, len(b_list), args.chunk):
        chunk = b_list[start:start + args.chunk]
        scores = unit_rows(kv, chunk) @ a_units.T
        for row, b in enumerate(chunk):
            for index in by_b[b]:
                true_score = scores[row, a_row[usable[index][0]]]
                rank_b_to_a[index] = int((scores[row] > true_score).sum()) + 1
    print(f"B->A ranks done ({time.time() - started:.0f}s)", flush=True)

    # A side: rank of the true B among the B records of its window.
    rank_a_to_b = np.empty(len(usable), dtype=np.int64)
    by_window: dict[int, list[int]] = {}
    for index, (_, b) in enumerate(usable):
        by_window.setdefault(window_of[b], []).append(index)
    for window, indices in by_window.items():
        members = [b for b in b_ids[window * args.window_count:(window + 1) * args.window_count] if b in kv]
        member_row = {b: i for i, b in enumerate(members)}
        scores = unit_rows(kv, [usable[i][0] for i in indices]) @ unit_rows(kv, members).T
        for row, index in enumerate(indices):
            true_score = scores[row, member_row[usable[index][1]]]
            rank_a_to_b[index] = int((scores[row] > true_score).sum()) + 1
    print(f"A->B ranks done ({time.time() - started:.0f}s)", flush=True)

    pair_counts = count_pairs(kv, a_units, a_known, b_ids, usable, window_of, args) if args.pair_windows else {}
    print(f"pair counts done ({time.time() - started:.0f}s)", flush=True)

    total = len(truth)
    report = {
        "model": model_path, "window_count": args.window_count, "truth_pairs": total,
        "usable_pairs": len(usable), **missing,
        "note": "recall over all truth pairs; final model, not each window's checkpoint",
        "recall_at_k": {
            "b_to_a (A in B's top-k)": recall_table(rank_b_to_a, total),
            "a_to_b (B in A's window top-k)": recall_table(rank_a_to_b, total),
            "mutual (both)": recall_table(np.maximum(rank_b_to_a, rank_a_to_b), total),
            "either": recall_table(np.minimum(rank_b_to_a, rank_a_to_b), total),
        },
        "median_rank": {"b_to_a": float(np.median(rank_b_to_a)), "a_to_b": float(np.median(rank_a_to_b))},
        "pairs_per_window": pair_counts,
        "seconds": round(time.time() - started, 1),
    }
    with open(os.path.join(args.output, "report.json"), "w", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    with open(os.path.join(args.output, "ranks.csv"), "w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["left_id", "right_id", "window", "rank_b_to_a", "rank_a_to_b"])
        for (a, b), r1, r2 in zip(usable, rank_b_to_a, rank_a_to_b):
            writer.writerow([a, b, window_of[b], int(r1), int(r2)])

    print(f"\nrecall@k over {total} truth pairs (k = {', '.join(map(str, KS))})")
    for name, table in report["recall_at_k"].items():
        print(f"  {name:32s} " + "  ".join(f"{value:.3f}" for value in table.values()))
    print(f"median rank: {report['median_rank']}; {report['seconds']} s")
    if pair_counts:
        print(f"\ncandidate pairs per window (mean of {pair_counts['windows']} sampled windows; "
              f"{pair_counts['true_pairs_per_window']} true pairs per window)")
        print(f"  {'rule':10s} " + "  ".join(f"{'k=' + k:>18s}" for k in pair_counts["k"]))
        for rule in ("b_to_a", "a_to_b", "mutual", "either"):
            cells = [f"{c['pairs']:>9.0f} ({c['true_share']:.3f})" for c in pair_counts[rule].values()]
            print(f"  {rule:10s} " + "  ".join(f"{cell:>18s}" for cell in cells))
        print("  (pairs, share of them that are true matches)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
