"""Simulate candidate-generation rules over the stream: which pairs would reach BERT?

Every window of the stream is a cross block of all A records x the window's B records (as in
the pipeline). With a run's final embedding model, this replays the windows in order and
evaluates, for each rule, the candidate pairs it would send to BERT:

  BT_k      B side: each B record's k most similar A records (over all A).
  RB_k      A side, "running best": a pair (A, B) is proposed when B enters A's k best B
            records seen so far in the stream (A matches at most one B in a dirty-clean
            stream, so a B that is no better than earlier ones cannot be A's match).
  RB_k+BT_j their union.
  MU_k      mutual top-k within the window (the current rule), for reference.

For each rule: recall over all true pairs, pairs per window (first window apart: the
running best starts empty), share of true pairs among them, and the same under a
similarity threshold tau (pairs with cosine >= tau). Uses the final model, not each window's
checkpoint: an approximation of what the run would have seen.

  python blocking_rules.py --model DIR --table-a A.csv --table-b B.csv --matches M.txt
      --window-count 6750 --output DIR
Outputs DIR/rules.json (tables) and DIR/rules-per-window.csv (pairs per window and rule).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from blocking_recall import read_ids, read_matches, resolve_model, unit_rows  # noqa: E402
from models.embedding_model import EmbeddingModel  # noqa: E402

RB_KS = (1, 2, 3)
BT_KS = (1, 3, 5, 10)
UNIONS = ((1, 1), (1, 3), (2, 1), (2, 3), (3, 3))
MU_K = 3
TAUS = tuple(round(0.3 + 0.05 * i, 2) for i in range(14))  # 0.30 .. 0.95
BINS = np.linspace(-1.0, 1.0, 401)  # 0.005-wide score bins
TOLERANCE = 1e-5  # float32 rounding between a matrix product and a recomputed dot product
CHUNK = 8192


class Rule:
    """Score histograms of the proposed pairs and of the true ones, plus pairs per window."""

    def __init__(self, windows: int):
        self.all = np.zeros(len(BINS) - 1, dtype=np.int64)
        self.true = np.zeros(len(BINS) - 1, dtype=np.int64)
        self.per_window = np.zeros(windows, dtype=np.int64)

    def add(self, window: int, scores: np.ndarray, true_scores: np.ndarray, sign: int = 1):
        self.all += sign * np.histogram(scores, BINS)[0]
        self.true += sign * np.histogram(true_scores, BINS)[0]
        self.per_window[window] += sign * len(scores)

    def table(self, total_true: int) -> dict:
        def at(tau):
            start = np.searchsorted(BINS, tau, side="left")
            pairs, hits = int(self.all[start:].sum()), int(self.true[start:].sum())
            return {"pairs": pairs, "recall": round(hits / total_true, 4),
                    "true_share": round(hits / pairs, 4) if pairs else 0.0}
        rest = self.per_window[1:]
        return {
            **at(-1.0),
            "pairs_per_window": round(float(self.per_window.mean()), 1),
            "first_window": int(self.per_window[0]),
            "per_window_after_first": round(float(rest.mean()), 1) if len(rest) else 0.0,
            "max_after_first": int(rest.max()) if len(rest) else 0,
            "last_10_windows": round(float(self.per_window[-10:].mean()), 1),
            "tau": {str(tau): at(tau) for tau in TAUS},
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--model", required=True)
    parser.add_argument("--table-a", required=True)
    parser.add_argument("--table-b", required=True)
    parser.add_argument("--matches", required=True)
    parser.add_argument("--window-count", type=int, required=True)
    parser.add_argument("--source-field", default="id")
    parser.add_argument("--left-prefix", default="A_")
    parser.add_argument("--right-prefix", default="B_")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.output, exist_ok=True)
    started = time.time()

    kv = EmbeddingModel.load(resolve_model(args.model), mmap="r").wv
    a_ids = [a for a in read_ids(args.table_a, args.source_field, args.left_prefix) if a in kv]
    b_ids = read_ids(args.table_b, args.source_field, args.right_prefix)
    truth = read_matches(args.matches, args.left_prefix, args.right_prefix)
    a_row = {a: i for i, a in enumerate(a_ids)}
    window_of = {b: i // args.window_count for i, b in enumerate(b_ids)}
    windows = (len(b_ids) + args.window_count - 1) // args.window_count
    usable = [(a_row[a], b) for a, b in truth if a in a_row and b in kv and b in window_of]
    truth_by_window: dict[int, list[tuple[int, str]]] = {}
    for a, b in usable:
        truth_by_window.setdefault(window_of[b], []).append((a, b))
    total_true = len(truth)
    a_units = unit_rows(kv, a_ids)
    n_a, ka, kb = len(a_ids), max(RB_KS), max(BT_KS)
    print(f"A={n_a} B={len(b_ids)} windows={windows} truth={total_true} usable={len(usable)} "
          f"({time.time() - started:.0f}s)", flush=True)

    rules = {name: Rule(windows) for name in
             [f"RB_{k}" for k in RB_KS] + [f"BT_{k}" for k in BT_KS]
             + [f"RB_{a}+BT_{b}" for a, b in UNIONS] + [f"MU_{MU_K}"]}
    running = np.full((n_a, ka), -np.inf, dtype=np.float32)  # each A's best scores so far, descending

    for window in range(windows):
        members = [b for b in b_ids[window * args.window_count:(window + 1) * args.window_count] if b in kv]
        if len(members) < kb:
            continue
        b_units = unit_rows(kv, members)
        m = len(members)
        col = {b: i for i, b in enumerate(members)}
        row_idx = np.empty((n_a, ka), dtype=np.int64)
        row_score = np.empty((n_a, ka), dtype=np.float32)
        col_best = np.full((kb, m), -np.inf, dtype=np.float32)
        for start in range(0, n_a, CHUNK):
            scores = a_units[start:start + CHUNK] @ b_units.T
            part = np.argpartition(-scores, ka - 1, axis=1)[:, :ka]
            part_scores = np.take_along_axis(scores, part, axis=1)
            order = np.argsort(-part_scores, axis=1)
            row_idx[start:start + len(scores)] = np.take_along_axis(part, order, axis=1)
            row_score[start:start + len(scores)] = np.take_along_axis(part_scores, order, axis=1)
            merged = np.concatenate([col_best, -np.partition(-scores, kb - 1, axis=0)[:kb]])
            col_best = -np.partition(-merged, kb - 1, axis=0)[:kb]
        col_sorted = -np.sort(-col_best, axis=0)  # row k-1: each B's k-th best A score

        pairs = truth_by_window.get(window, [])
        t_rows = np.array([a for a, _ in pairs], dtype=np.int64)
        t_cols = np.array([col[b] for _, b in pairs], dtype=np.int64)
        t_scores = np.sum(a_units[t_rows] * b_units[t_cols], axis=1) if pairs else np.zeros(0, np.float32)
        true_codes = t_rows * m + t_cols
        rank_in_row = np.full(len(pairs), ka + 1)  # B's rank in A's window list (ka + 1: beyond)
        for position in reversed(range(ka)):
            rank_in_row[row_idx[t_rows, position] == t_cols] = position + 1

        # B side: per B, its k best A (scores col_sorted[:k]); true when the true A reaches the k-th score.
        bt_true = {}
        for k in BT_KS:
            bt_true[k] = t_scores >= col_sorted[k - 1][t_cols] - TOLERANCE
            rules[f"BT_{k}"].add(window, col_sorted[:k].ravel(), t_scores[bt_true[k]])

        # A side, running best: window entries that enter A's k best so far.
        rb_mask, rb_true = {}, {}
        for k in RB_KS:
            union = np.concatenate([running[:, :k], row_score[:, :k]], axis=1)
            kth = -np.partition(-union, k - 1, axis=1)[:, k - 1]
            rb_mask[k] = row_score[:, :k] >= kth[:, None]
            proposed_codes = (np.arange(n_a)[:, None] * m + row_idx[:, :k])[rb_mask[k]]
            rb_true[k] = np.isin(true_codes, proposed_codes)
            rules[f"RB_{k}"].add(window, row_score[:, :k][rb_mask[k]], t_scores[rb_true[k]])

        for a, b in UNIONS:
            name = f"RB_{a}+BT_{b}"
            rules[name].add(window, row_score[:, :a][rb_mask[a]], t_scores[rb_true[a]])
            rules[name].add(window, col_sorted[:b].ravel(), t_scores[bt_true[b] & ~rb_true[a]])
            # Pairs proposed by both sides are counted once.
            both = rb_mask[a] & (row_score[:, :a] >= col_sorted[b - 1][row_idx[:, :a]] - TOLERANCE)
            rules[name].add(window, row_score[:, :a][both], np.zeros(0), sign=-1)

        mu = (row_score[:, :MU_K] >= col_sorted[MU_K - 1][row_idx[:, :MU_K]] - TOLERANCE)
        rules[f"MU_{MU_K}"].add(window, row_score[:, :MU_K][mu], t_scores[bt_true[MU_K] & (rank_in_row <= MU_K)])

        top = np.concatenate([running, row_score], axis=1)
        running = -np.sort(-top, axis=1)[:, :ka]
        if window % 10 == 0:
            print(f"  window {window}/{windows} ({time.time() - started:.0f}s)", flush=True)

    report = {"window_count": args.window_count, "windows": windows, "truth_pairs": total_true,
              "true_pairs_per_window": round(len(usable) / windows, 1),
              "note": "final model; pairs counted once per rule; tau keeps pairs with cosine >= tau",
              "rules": {name: rule.table(total_true) for name, rule in rules.items()},
              "seconds": round(time.time() - started, 1)}
    with open(os.path.join(args.output, "rules.json"), "w", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
    with open(os.path.join(args.output, "rules-per-window.csv"), "w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["window", *rules])
        for window in range(windows):
            writer.writerow([window, *(int(rule.per_window[window]) for rule in rules.values())])

    print(f"\n{'rule':12s} {'recall':>7s} {'true%':>6s} {'pairs/win':>10s} {'1st win':>8s} {'after 1st':>10s} "
          f"{'last 10':>8s} {'max':>7s}")
    for name, table in report["rules"].items():
        print(f"{name:12s} {table['recall']:7.3f} {100 * table['true_share']:6.2f} {table['pairs_per_window']:10.0f} "
              f"{table['first_window']:8d} {table['per_window_after_first']:10.0f} {table['last_10_windows']:8.0f} "
              f"{table['max_after_first']:7d}")
    print("\nwith a similarity threshold tau (recall / pairs per window):")
    print(f"{'rule':12s} " + " ".join(f"{t:>12s}" for t in ("0.5", "0.6", "0.7", "0.8")))
    for name, table in report["rules"].items():
        cells = [f"{table['tau'][t]['recall']:.3f}/{table['tau'][t]['pairs'] / windows:6.0f}" for t in ("0.5", "0.6", "0.7", "0.8")]
        print(f"{name:12s} " + " ".join(f"{c:>12s}" for c in cells))
    print(f"\n{report['seconds']} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
