#!/usr/bin/env python3
"""Score labelled candidate pairs with a trained BERT matcher and pick its threshold.

Reads a run's embedding decision (predicted_matching.csv: left_id,right_id,score), labels
each pair with the ground truth, and asks the BERT checkpoint for its match probability.
Record texts are built with the pipeline's own serialization (pipeline.record_store), and
the model is loaded with the pipeline's InferenceService, so the probabilities are the ones
the bert-matching worker would compute.

Outputs (in --output):
  pairs.csv    left_id, right_id, embedding_score, bert_probability, label
  report.json  AUC of BERT and of the embedding score, and per-threshold metrics
and prints the threshold table. "pass rate" is the share of false pairs BERT keeps;
"projected" metrics assume these pairs are the whole prediction (decision mode).

Runs where the BERT image runs (exp/run_bert_threshold_eval.sh starts it as a Job).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

import numpy as np
import pandas as pd

from pipeline.record_store import DEFAULT_MAX_TEXT_CHARS, record_texts

THRESHOLDS = (0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.999)


def read_truth(path: str) -> set[tuple[str, str]]:
    frame = pd.read_csv(path, header=None, dtype=str, usecols=[0, 1], names=["left", "right"])
    # Tolerate a header row: real ids carry the A_/B_ prefixes.
    frame = frame[~frame["left"].str.lower().isin({"left_id", "ltable_id", "id1", "a"})]
    return set(zip(frame["left"].str.strip(), frame["right"].str.strip()))


def read_texts(path: str, wanted: set[str], source_field: str, max_chars: int) -> dict[str, str]:
    """Texts of the rows whose ``source_field`` is in ``wanted``, read in chunks.

    Values are parsed by pandas as normalization parses its input, so the texts match the
    record store's.
    """
    config = {"record_ids": {"source_field": source_field}}
    texts: dict[str, str] = {}
    for chunk in pd.read_csv(path, chunksize=200_000):
        ids = chunk[source_field].astype(str).str.strip()
        rows = chunk[ids.isin(wanted)]
        if len(rows):
            texts.update(record_texts(config, rows, rows[source_field].astype(str).str.strip(), max_chars))
    return texts


def auc(scores: np.ndarray, labels: np.ndarray) -> float | None:
    """Probability that a random true pair outranks a random false one (ties count half)."""
    positives, negatives = int(labels.sum()), int((1 - labels).sum())
    if not positives or not negatives:
        return None
    ranks = pd.Series(scores).rank(method="average").to_numpy()
    return float((ranks[labels == 1].sum() - positives * (positives + 1) / 2) / (positives * negatives))


def threshold_rows(probabilities: np.ndarray, labels: np.ndarray, total_true: int) -> list[dict]:
    true_pairs, false_pairs = int(labels.sum()), int((1 - labels).sum())
    rows = []
    for threshold in THRESHOLDS:
        kept = probabilities >= threshold
        tp, fp = int((kept & (labels == 1)).sum()), int((kept & (labels == 0)).sum())
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / total_true if total_true else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        rows.append({
            "threshold": threshold, "kept": tp + fp, "true_kept": tp, "false_kept": fp,
            "true_retention": tp / true_pairs if true_pairs else 0.0,
            "false_pass_rate": fp / false_pairs if false_pairs else 0.0,
            "projected_precision": precision, "projected_recall": recall, "projected_f1": f1,
        })
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--model-dir", required=True, help="BERT checkpoint (bert-dir/<version_name>)")
    parser.add_argument("--pairs", required=True, help="predicted_matching.csv of a run")
    parser.add_argument("--matches", required=True, help="ground truth, two columns: left_id,right_id")
    parser.add_argument("--table-a", required=True)
    parser.add_argument("--table-b", required=True)
    parser.add_argument("--source-field", default="id", help="record_ids.source_field (column with A_/B_ ids)")
    parser.add_argument("--output", required=True)
    parser.add_argument("--device", default="auto", choices=("auto", "cpu", "cuda"))
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--max-length", type=int, default=128, help="bert_matching.max_length")
    parser.add_argument("--max-text-chars", type=int, default=DEFAULT_MAX_TEXT_CHARS)
    parser.add_argument("--limit", type=int, default=None, help="score a random sample of N pairs (quick check)")
    args = parser.parse_args()

    started = time.time()
    os.makedirs(args.output, exist_ok=True)
    pairs = pd.read_csv(args.pairs, dtype={"left_id": str, "right_id": str})
    if args.limit and args.limit < len(pairs):
        pairs = pairs.sample(n=args.limit, random_state=1729).reset_index(drop=True)
    truth = read_truth(args.matches)
    pairs["label"] = [int((left, right) in truth) for left, right in zip(pairs.left_id, pairs.right_id)]
    print(f"pairs={len(pairs)} true={int(pairs.label.sum())} false={int((1 - pairs.label).sum())} "
          f"ground truth={len(truth)}", flush=True)

    texts = read_texts(args.table_a, set(pairs.left_id), args.source_field, args.max_text_chars)
    texts.update(read_texts(args.table_b, set(pairs.right_id), args.source_field, args.max_text_chars))
    missing = sorted((set(pairs.left_id) | set(pairs.right_id)) - set(texts))
    if missing:
        print(f"[error] {len(missing)} ids have no record text, e.g. {missing[:5]}", file=sys.stderr)
        return 1
    print(f"texts ready: {len(texts)} records ({time.time() - started:.0f}s)", flush=True)

    from pipeline.bert_inference import InferenceService

    service = InferenceService(save_dir=args.model_dir, max_length=args.max_length, device=args.device)
    probabilities = []
    step = 20_000
    for start in range(0, len(pairs), step):
        part = pairs.iloc[start:start + step]
        probabilities.extend(service.predict_batch(
            [texts[left] for left in part.left_id], [texts[right] for right in part.right_id],
            batch_size=args.batch_size,
        ))
        print(f"scored {min(start + step, len(pairs))}/{len(pairs)} ({time.time() - started:.0f}s)", flush=True)
    pairs["bert_probability"] = probabilities
    pairs = pairs.rename(columns={"score": "embedding_score"})
    pairs[["left_id", "right_id", "embedding_score", "bert_probability", "label"]].to_csv(
        os.path.join(args.output, "pairs.csv"), index=False)

    labels = pairs.label.to_numpy()
    probs = pairs.bert_probability.to_numpy(dtype=float)
    rows = threshold_rows(probs, labels, len(truth))
    best = max(rows, key=lambda row: row["projected_f1"])
    report = {
        "pairs": len(pairs), "true_pairs": int(labels.sum()), "ground_truth": len(truth),
        "bert_auc": auc(probs, labels),
        "embedding_score_auc": auc(pairs.embedding_score.to_numpy(dtype=float), labels),
        "probability_quantiles": {
            name: {str(q): float(np.quantile(probs[labels == value], q)) for q in (0.05, 0.25, 0.5, 0.75, 0.95)}
            for name, value in (("true", 1), ("false", 0)) if (labels == value).any()
        },
        "thresholds": rows,
        "best_f1_threshold": best["threshold"],
        "lowest_threshold_with_false_pass_rate_at_most": {
            str(limit): next((row["threshold"] for row in rows if row["false_pass_rate"] <= limit), None)
            for limit in (0.2, 0.1, 0.05)
        },
        "seconds": round(time.time() - started, 1),
    }
    with open(os.path.join(args.output, "report.json"), "w", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)

    print(f"\nAUC  bert={report['bert_auc']:.4f}  embedding score={report['embedding_score_auc']:.4f}")
    print(f"{'thr':>6} {'kept':>7} {'true':>7} {'false':>7} {'true ret':>9} {'false pass':>10} "
          f"{'proj P':>7} {'proj R':>7} {'proj F1':>7}")
    for row in rows:
        print(f"{row['threshold']:>6} {row['kept']:>7} {row['true_kept']:>7} {row['false_kept']:>7} "
              f"{row['true_retention']:>9.3f} {row['false_pass_rate']:>10.3f} {row['projected_precision']:>7.3f} "
              f"{row['projected_recall']:>7.3f} {row['projected_f1']:>7.3f}")
    print(f"\nbest projected F1 at threshold {best['threshold']}; "
          f"false pass <=10% from threshold {report['lowest_threshold_with_false_pass_rate_at_most']['0.1']}")
    print(f"report: {os.path.join(args.output, 'report.json')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
