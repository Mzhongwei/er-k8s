#!/usr/bin/env python3
"""TF-IDF candidate generation + BERT matching over a stream, in one process (no Kubernetes,
Kafka or embedding stages), with the pipeline's own functions:

  1. index:    TF-IDF of table A (pipeline.tfidf_blocking), terms as normalization stores them
  2. BERT:     trained on the dataset's train/validation pairs (pipeline.bert_training)
  3. stream:   table B in windows; each window's running-best TF-IDF candidates
               (pipeline.calculating_similarity.score_running_best_tfidf_pairs) are judged by
               BERT (pipeline.bert_inference); the output is the capacity-constrained selection
               among all pairs accepted (pipeline.bert_matching.select_matches)
  4. metrics:  candidate recall, precision/recall/F1 of the output against matches.txt, and
               the F1 a threshold sweep would reach (an optimistic reference, chosen on the
               ground truth), time per stage

  python exp-caise/run_demo.py --data-root /data --output reports/caise-demo [--datasets a,b]
      [--no-bert] [--device cuda] [--threshold 0.5]

Outputs per dataset in OUTPUT/<dataset>/: result.json, candidates.csv (every candidate with its
TF-IDF score, BERT probability and window), windows.jsonl, the BERT model (bert/, reused
on a rerun). OUTPUT/summary.csv collects one row per dataset. Note: BERT's training pairs are
labelled with the same ground truth that evaluates the output.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CODE = ROOT / "code/Energy-Aware-Entity-Resolution"
sys.path.insert(0, str(CODE))

import pandas as pd  # noqa: E402
import yaml  # noqa: E402

THRESHOLDS = [round(0.05 * i, 2) for i in range(2, 20)]  # 0.10 .. 0.95


def first_file(directory: Path, *names: str) -> Path:
    for name in names:
        if (directory / name).is_file():
            return directory / name
    raise FileNotFoundError(f"none of {names} in {directory}")


def dataset_settings(path: Path, names: list[str] | None) -> dict[str, dict]:
    spec = yaml.safe_load(path.read_text())
    defaults = spec.get("defaults") or {}
    datasets = {name: {**defaults, **(values or {})} for name, values in spec["datasets"].items()}
    if names:
        unknown = [name for name in names if name not in datasets]
        if unknown:
            raise SystemExit(f"unknown datasets {unknown}; known: {', '.join(datasets)}")
        datasets = {name: datasets[name] for name in names}
    return datasets


def read_matches(path: Path) -> set[tuple[str, str]]:
    pairs = set()
    for line in path.read_text().splitlines():
        if line.strip():
            left, right = line.strip().split(",")[:2]
            pairs.add((left, right))
    return pairs


def scores(predicted: set, truth: set) -> dict:
    correct = len(predicted & truth)
    precision = correct / len(predicted) if predicted else 0.0
    recall = correct / len(truth) if truth else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"correct": correct, "predicted": len(predicted), "truth": len(truth),
            "precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4)}


def train_bert(config: dict, directory: Path, out: Path, epochs: int, device: str) -> tuple[Path, float]:
    """Train (or reuse) the dataset's BERT matcher; returns its directory and training seconds."""
    model_dir = out / "bert"
    if (model_dir / "config.json").is_file():
        print(f"[demo] reusing BERT model {model_dir}", flush=True)
        return model_dir, 0.0
    from pipeline.bert_training import train_model
    from pipeline.normalization import sequence_generating_m1

    started = time.perf_counter()
    # As entries/batch/BertTrai-normalization.py: pandas defaults, one text per side.
    processed = {
        "train": sequence_generating_m1(pd.read_csv(first_file(directory, "train.csv"))),
        "eval": sequence_generating_m1(pd.read_csv(first_file(directory, "valid.csv", "eval.csv"))),
    }
    training = {**config, "bert_training": {"device": device, "epochs": epochs, "exp_name": "checkpoints"}}
    cwd = os.getcwd()
    os.chdir(out)  # the Trainer writes its epoch checkpoints under ./pipeline/<exp_name>
    try:
        trainer, tokenizer = train_model(training, processed)
    finally:
        os.chdir(cwd)
    trainer.save_model(str(model_dir))
    tokenizer.save_pretrained(str(model_dir))
    return model_dir, time.perf_counter() - started


def run_dataset(name: str, settings: dict, args) -> dict:
    from pipeline.bert_matching import select_matches
    from pipeline.calculating_similarity import RunningBest, score_running_best_tfidf_pairs
    from pipeline.record_store import record_term_texts, record_texts
    from pipeline.tfidf_blocking import TfidfIndex, tfidf_config

    directory = args.data_root / settings["dir"]
    out = args.output / name
    out.mkdir(parents=True, exist_ok=True)
    config = {
        "record_ids": {"source_field": "id"},
        "calculating_similarity": {"tfidf": {"fields": settings.get("fields"), "max_df": args.max_df}},
    }
    tfidf = tfidf_config(config)
    timing = {}

    # As utils/record_batches.py reads the sources: pandas defaults.
    table_a = pd.read_csv(first_file(directory, "tableA.csv", "table_A.csv"))
    table_b = pd.read_csv(first_file(directory, "tableB.csv", "table_B.csv"))
    a_ids = table_a["id"].astype(str).tolist()
    b_ids = table_b["id"].astype(str).tolist()
    truth = {pair for pair in read_matches(directory / "matches.txt")}
    present_a, present_b = set(a_ids), set(b_ids)
    truth = {(left, right) for left, right in truth if left in present_a and right in present_b}
    print(f"[demo] {name}: A={len(a_ids)} B={len(b_ids)} true pairs={len(truth)}", flush=True)

    started = time.perf_counter()
    a_terms = record_term_texts(config, table_a, a_ids)
    b_terms = record_term_texts(config, table_b, b_ids)
    index = TfidfIndex.from_records(a_terms, max_df=tfidf["max_df"])
    timing["index_s"] = time.perf_counter() - started
    print(f"[demo] TF-IDF index: {len(index.ids)} records, {len(index.vocabulary)} terms "
          f"({timing['index_s']:.1f} s)", flush=True)

    service = None
    if args.bert:
        from pipeline.bert_inference import InferenceService

        model_dir, timing["bert_training_s"] = train_bert(
            config, directory, out, int(args.epochs or settings["bert_epochs"]), args.device)
        service = InferenceService(save_dir=str(model_dir), max_length=args.max_length, device=args.device)
        a_texts = record_texts(config, table_a, a_ids)
        b_texts = record_texts(config, table_b, b_ids)
    del table_a, table_b

    state = RunningBest(int(settings["k"]))
    window = int(settings["window"])
    judged = []  # (left, right, tfidf score, probability, window)
    timing["candidates_s"] = timing["bert_matching_s"] = 0.0
    with (out / "windows.jsonl").open("w") as log:
        for w, start in enumerate(range(0, len(b_ids), window)):
            members = b_ids[start:start + window]
            tick = time.perf_counter()
            pairs = score_running_best_tfidf_pairs(
                index, members, [b_terms[x] for x in members], state, window=w,
                min_similarity=args.min_similarity, query_top_k=int(settings["query_top_k"]),
                max_pairs=args.pairs_per_window, chunk_rows=tfidf["chunk_rows"],
            )
            candidates_s = time.perf_counter() - tick
            tick = time.perf_counter()
            if service is not None and pairs:
                probabilities = service.predict_batch(
                    [a_texts[left] for left, _, _ in pairs], [b_texts[right] for _, right, _ in pairs],
                    batch_size=args.batch_size)
            else:
                probabilities = [1.0] * len(pairs)
            bert_s = time.perf_counter() - tick
            judged.extend((left, right, score, p, w) for (left, right, score), p in zip(pairs, probabilities))
            timing["candidates_s"] += candidates_s
            timing["bert_matching_s"] += bert_s
            log.write(json.dumps({"window": w, "records": len(members), "pairs": len(pairs),
                                  "candidates_s": round(candidates_s, 3), "bert_s": round(bert_s, 3)}) + "\n")
            if w % 20 == 0:
                print(f"[demo] window {w}: {len(pairs)} pairs, candidates {candidates_s:.2f} s, "
                      f"BERT {bert_s:.2f} s", flush=True)

    with (out / "candidates.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["left_id", "right_id", "tfidf_score", "bert_probability", "window", "true"])
        for left, right, score, p, w in judged:
            writer.writerow([left, right, f"{score:.5f}", f"{p:.5f}", w, int((left, right) in truth)])

    max_left, max_right = settings["max_matches"]
    windows = -(-len(b_ids) // window)

    def output_at(threshold):
        accepted = [(left, right, score, p) for left, right, score, p, _ in judged if p >= threshold]
        return {(left, right) for left, right, _, _ in select_matches(accepted, max_left, max_right)}

    result = {
        "dataset": name, "settings": settings, "max_df": tfidf["max_df"],
        "min_similarity": args.min_similarity, "pairs_per_window": args.pairs_per_window,
        "windows": windows, "candidate_pairs": len(judged),
        "candidate_pairs_per_window": round(len(judged) / windows, 1),
        "candidate_recall": scores({(l, r) for l, r, *_ in judged}, truth)["recall"],
        "timing_s": {key: round(value, 2) for key, value in timing.items()},
    }
    if args.bert:
        result["threshold"] = args.threshold
        result["matching"] = scores(output_at(args.threshold), truth)
        sweep = {str(t): scores(output_at(t), truth)["f1"] for t in THRESHOLDS}
        best = max(sweep, key=sweep.get)
        result["threshold_sweep_f1"] = sweep
        result["best_threshold_on_truth"] = {"threshold": float(best), "f1": sweep[best]}
    (out / "result.json").write_text(json.dumps(result, indent=2))
    print(json.dumps({k: v for k, v in result.items() if k not in ("settings", "threshold_sweep_f1")}, indent=2),
          flush=True)
    return result


def check_dataset(name: str, settings: dict, data_root: Path) -> list[str]:
    """Problems with a dataset's files (empty: ready); prints what was found."""
    directory = data_root / settings["dir"]
    if not directory.is_dir():
        print(f"[check] FAIL {name:22s} {directory}\n         - directory does not exist", flush=True)
        return ["directory does not exist"]
    problems, found = [], {}
    for key, names in (("A", ("tableA.csv", "table_A.csv")), ("B", ("tableB.csv", "table_B.csv")),
                       ("matches", ("matches.txt",)), ("train", ("train.csv",)),
                       ("valid", ("valid.csv", "eval.csv"))):
        try:
            found[key] = first_file(directory, *names)
        except FileNotFoundError:
            problems.append(f"missing {' or '.join(names)}")
    tables = {}
    for side in ("A", "B"):
        if side in found:
            columns = list(pd.read_csv(found[side], nrows=0).columns)
            if "id" not in columns:
                problems.append(f"table {side} has no id column ({columns})")
                continue
            ids = pd.read_csv(found[side], usecols=["id"])["id"].astype(str)
            if ids.duplicated().any():
                problems.append(f"table {side}: {int(ids.duplicated().sum())} duplicate ids")
            missing_fields = [f for f in settings.get("fields") or [] if f not in [c.lower() for c in columns]]
            if missing_fields:
                problems.append(f"table {side} lacks TF-IDF fields {missing_fields}")
            tables[side] = set(ids)
    line = f"A={len(tables.get('A', ()))} B={len(tables.get('B', ()))}"
    if "matches" in found:
        pairs = read_matches(found["matches"])
        if not pairs:
            problems.append("matches.txt is empty")
        elif len(tables) == 2:
            present = sum(left in tables["A"] and right in tables["B"] for left, right in pairs)
            line += f" matches={len(pairs)} (ids in tables: {present / len(pairs):.0%})"
            if present / len(pairs) < 0.5:
                problems.append("fewer than half of matches.txt's ids are in the tables (swapped sides or wrong files?)")
    for key in ("train", "valid"):
        if key in found:
            frame = pd.read_csv(found[key])
            sides = {column.split(".")[0] for column in frame.columns if "." in column}
            if "label" not in frame.columns or not {"table1", "table2"} <= sides:
                problems.append(f"{found[key].name} needs label, table1.* and table2.* columns")
            else:
                line += f" {found[key].name}={len(frame)} ({int(frame['label'].sum())} positive)"
    print(f"[check] {'OK  ' if not problems else 'FAIL'} {name:22s} {directory}  {line}", flush=True)
    for problem in problems:
        print(f"         - {problem}", flush=True)
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-root", type=Path, default=Path("/data"))
    parser.add_argument("--datasets-file", type=Path, default=Path(__file__).with_name("datasets.yaml"))
    parser.add_argument("--datasets", help="comma-separated names from the datasets file (default: all)")
    parser.add_argument("--check", action="store_true", help="only check that every dataset's files are in place")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--no-bert", dest="bert", action="store_false", help="candidates only")
    parser.add_argument("--device", default="auto", choices=["auto", "cuda", "cpu"])
    parser.add_argument("--epochs", type=int, help="BERT epochs (default: per dataset)")
    parser.add_argument("--threshold", type=float, default=0.5, help="BERT acceptance probability")
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--max-length", type=int, default=128)
    parser.add_argument("--max-df", type=float, default=0.1, help="drop tokens in more than this share of A")
    parser.add_argument("--min-similarity", type=float, help="drop candidates below this TF-IDF cosine")
    parser.add_argument("--pairs-per-window", type=int, help="keep at most this many candidates per window")
    args = parser.parse_args()
    args.data_root = args.data_root.resolve()
    names = args.datasets.split(",") if args.datasets else None
    if args.check:
        failed = [name for name, settings in dataset_settings(args.datasets_file, names).items()
                  if check_dataset(name, settings, args.data_root)]
        print(f"[check] {'all datasets ready' if not failed else 'not ready: ' + ', '.join(failed)}")
        return 1 if failed else 0
    if args.output is None:
        parser.error("--output is required (except with --check)")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)

    rows = []
    for name, settings in dataset_settings(args.datasets_file, names).items():
        result = run_dataset(name, settings, args)
        matching = result.get("matching", {})
        rows.append({
            "dataset": name, "windows": result["windows"],
            "candidate_pairs_per_window": result["candidate_pairs_per_window"],
            "candidate_recall": result["candidate_recall"],
            "precision": matching.get("precision"), "recall": matching.get("recall"), "f1": matching.get("f1"),
            "best_threshold_on_truth": (result.get("best_threshold_on_truth") or {}).get("threshold"),
            "best_f1_on_truth": (result.get("best_threshold_on_truth") or {}).get("f1"),
            **{key: value for key, value in result["timing_s"].items()},
        })
        with (args.output / "summary.csv").open("w", newline="") as stream:
            columns = list(dict.fromkeys(key for row in rows for key in row))
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
    print(f"[demo] summary: {args.output / 'summary.csv'}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
