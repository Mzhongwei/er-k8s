#!/usr/bin/env python3
"""Build a BERT training set from a run's embedding decision, holding its records out.

The decision's false pairs are the negatives BERT meets in production (embedding near
neighbours, often product variants), unlike random pairs. To train on them without testing
on records BERT has seen, a set of B records is held out of the stream: their decision
pairs (true -> positive, false -> hard negative) and all their true matches become labelled
pairs, and they are removed from table_B and matches.txt. Train and eval are split by B
record, so eval also contains no B seen in training.

Held-out B records:
  - --holdout-fraction of the B records in the decision (their pairs give hard negatives);
  - --extra-positive-b matched B records outside the decision (more positives).
The previous train.csv/eval.csv (their B records are already out of the stream) are appended
to train unless --no-previous, with ids prefixed "prev-" (the stream was renumbered since).

Writes to --output-dir: table_A.csv (copied), table_B.csv and matches.txt without the
held-out B records, train.csv, eval.csv (same columns as before), stats.json.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
from collections import defaultdict
from pathlib import Path

import pandas as pd


def read_matches(path: Path) -> list[tuple[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        return [(row[0].strip(), row[1].strip()) for row in csv.reader(stream) if len(row) >= 2]


def labelled_rows(pairs, labels, table_a: pd.DataFrame, table_b: pd.DataFrame) -> pd.DataFrame:
    """Rows in the train.csv layout: id, label, table1.<A columns>, table2.<B columns>."""
    left = table_a.loc[[a for a, _ in pairs]].reset_index()
    right = table_b.loc[[b for _, b in pairs]].reset_index()
    left.columns = [f"table1.{column}" for column in left.columns]
    right.columns = [f"table2.{column}" for column in right.columns]
    frame = pd.concat([left, right], axis=1)
    frame.insert(0, "label", [str(label) for label in labels])
    return frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-dir", type=Path, required=True,
                        help="dataset with table_A.csv, table_B.csv, matches.txt, train.csv, eval.csv")
    parser.add_argument("--pairs", type=Path, required=True, help="predicted_matching.csv of a run on that dataset")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--holdout-fraction", type=float, default=0.15)
    parser.add_argument("--extra-positive-b", type=int, default=3000)
    parser.add_argument("--eval-fraction", type=float, default=0.2)
    parser.add_argument("--no-previous", action="store_true", help="do not append the previous train/eval")
    parser.add_argument("--id-column", default="id")
    parser.add_argument("--seed", type=int, default=1729)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    source, output = args.source_dir, args.output_dir
    output.mkdir(parents=True, exist_ok=True)

    matches = read_matches(source / "matches.txt")
    truth = set(matches)
    a_of_b: dict[str, set[str]] = defaultdict(set)
    for a, b in matches:
        a_of_b[b].add(a)
    decision = pd.read_csv(args.pairs, dtype=str)
    decision_pairs = list(zip(decision.left_id, decision.right_id))

    decision_b = sorted({b for _, b in decision_pairs})
    held = set(rng.sample(decision_b, round(len(decision_b) * args.holdout_fraction)))
    outside = sorted(set(a_of_b) - set(decision_b))
    held |= set(rng.sample(outside, min(args.extra_positive_b, len(outside))))

    positives = sorted((a, b) for b in held for a in a_of_b.get(b, ()))
    negatives = sorted((a, b) for a, b in decision_pairs if b in held and (a, b) not in truth)
    held_list = sorted(held)
    rng.shuffle(held_list)
    eval_b = set(held_list[: round(len(held_list) * args.eval_fraction)])

    table_a = pd.read_csv(source / "table_A.csv", dtype=str, keep_default_na=False).set_index(args.id_column)
    kept_rows = 0
    held_rows = []
    with (output / "table_B.csv").open("w", encoding="utf-8", newline="") as stream:
        header = True
        for chunk in pd.read_csv(source / "table_B.csv", dtype=str, keep_default_na=False, chunksize=200_000):
            is_held = chunk[args.id_column].isin(held)
            held_rows.append(chunk[is_held])
            kept = chunk[~is_held]
            kept.to_csv(stream, index=False, header=header)
            kept_rows += len(kept)
            header = False
    table_b = pd.concat(held_rows).set_index(args.id_column)
    missing = held - set(table_b.index)
    if missing:
        raise SystemExit(f"{len(missing)} held-out B records are not in table_B.csv, e.g. {sorted(missing)[:3]}")

    with (output / "matches.txt").open("w", encoding="utf-8", newline="") as stream:
        csv.writer(stream, lineterminator="\n").writerows((a, b) for a, b in matches if b not in held)
    shutil.copyfile(source / "table_A.csv", output / "table_A.csv")

    splits = {}
    for name, in_split in (("train", lambda b: b not in eval_b), ("eval", lambda b: b in eval_b)):
        pairs = [(pair, 1) for pair in positives if in_split(pair[1])]
        pairs += [(pair, 0) for pair in negatives if in_split(pair[1])]
        rng.shuffle(pairs)
        splits[name] = labelled_rows([p for p, _ in pairs], [label for _, label in pairs], table_a, table_b)
    previous = 0
    if not args.no_previous:
        old = [pd.read_csv(source / name, dtype=str, keep_default_na=False).drop(columns=["id"])
               for name in ("train.csv", "eval.csv")]
        # Their B records left the stream when that set was built, and the stream's B ids were
        # renumbered then: these ids name other records now. Training reads only the texts.
        for frame in old:
            for column in ("table1.id", "table2.id"):
                frame[column] = "prev-" + frame[column]
        previous = sum(len(frame) for frame in old)
        splits["train"] = pd.concat([splits["train"], *old], ignore_index=True)
        splits["train"] = splits["train"].sample(frac=1, random_state=args.seed).reset_index(drop=True)
    for name, frame in splits.items():
        frame.insert(0, "id", range(len(frame)))
        frame.to_csv(output / f"{name}.csv", index=False)

    stats = {
        "source": str(source), "decision_pairs": str(args.pairs), "seed": args.seed,
        "held_out_b": len(held), "held_out_b_from_decision": len(held & set(decision_b)),
        "eval_b": len(eval_b), "positives": len(positives), "hard_negatives": len(negatives),
        "previous_pairs_in_train": previous,
        **{f"{name}_rows": len(frame) for name, frame in splits.items()},
        **{f"{name}_positives": int((frame.label == "1").sum()) for name, frame in splits.items()},
        "table_B_rows": kept_rows, "matches": sum(1 for _, b in matches if b not in held),
        "matches_before": len(matches),
    }
    (output / "stats.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
