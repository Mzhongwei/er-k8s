"""Replay one window of a stage from a saved state and time its phases (E1b, E2).

Runs inside the pipeline images (exp/run_replay.sh starts it as a Kubernetes Job), on a
state saved during a run with --replay-windows (replay/w<ordinal>/ under the embedding model
directory, candidates_w<ordinal>.csv under the predicted-match directory).

  word2vec  load the model as it was before window w (a restart restores it the same way),
            train window w's walks with N Gensim workers, write the checkpoint as the stage
            does (then delete it)
  bert      load the BERT matcher, judge window w's candidate pairs on a device

Every repetition prints one line "[REPLAY] {json}" with Unix times of its phases, which
exp/replay_analyze.py turns into time and RAPL energy per phase.
"""
from __future__ import annotations

import time

PROCESS_START = time.time()

import argparse  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import shutil  # noqa: E402
import sys  # noqa: E402

sys.path.insert(0, "/app")


def emit(record: dict) -> None:
    print("[REPLAY] " + json.dumps(record), flush=True)


def replay_word2vec(args) -> None:
    from models.embedding_model import EmbeddingModel
    from pipeline.embedding_training import WalkShardCorpus, is_walk_shard_manifest, retrain_embeddings, set_workers
    imports_done = time.time()

    with open(os.path.join(args.state, "config.json"), encoding="utf-8") as stream:
        config = json.load(stream)
    with open(os.path.join(args.state, "walks.json"), encoding="utf-8") as stream:
        manifest = json.load(stream)
    model_path = os.path.join(args.state, "model", "embedding.emb")
    for rep in range(1, args.reps + 1):
        t0 = time.time()
        model = EmbeddingModel.load(model_path)  # full load: training needs writable arrays
        t_loaded = time.time()
        set_workers(model, args.workers)
        sequences = WalkShardCorpus(manifest) if is_walk_shard_manifest(manifest) else manifest
        model = retrain_embeddings(config, model, sequences)
        t_trained = time.time()
        target = os.path.join(args.scratch, f"w{args.workers}-r{rep}")
        model.save(os.path.join(target, "embedding.emb"))
        t_written = time.time()
        shutil.rmtree(target, ignore_errors=True)
        emit({
            "mode": "word2vec", "state": args.label, "workers": args.workers, "rep": rep,
            "process_start": PROCESS_START, "imports_done": imports_done,
            "start": t0, "loaded": t_loaded, "trained": t_trained, "written": t_written,
            "vocabulary": len(model.wv.key_to_index),
        })
        del model, sequences


def replay_bert(args) -> None:
    import torch
    from pipeline.bert_matching import BertMatcher
    from utils.utils import load_scored_pairs
    imports_done = time.time()

    with open(args.config, encoding="utf-8") as stream:
        config = json.load(stream)
    device, _, cores = args.device.partition(":")
    config["bert_matching"] = dict(config.get("bert_matching") or {}, device=device)
    if device == "cpu":
        torch.set_num_threads(int(cores or 2))
    pairs = load_scored_pairs(config, args.pairs)
    for rep in range(1, args.reps + 1):
        t0 = time.time()
        matcher = BertMatcher(config)
        t_loaded = time.time()
        accepted = matcher.judge(pairs, remember=False)
        if device == "cuda":
            torch.cuda.synchronize()
        t_judged = time.time()
        emit({
            "mode": "bert", "state": args.label, "device": args.device, "rep": rep, "pairs": len(pairs),
            "accepted": len(accepted), "process_start": PROCESS_START, "imports_done": imports_done,
            "start": t0, "loaded": t_loaded, "judged": t_judged,
        })
        del matcher


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)
    w2v = sub.add_parser("word2vec")
    w2v.add_argument("--state", required=True, help="replay/w<ordinal> directory")
    w2v.add_argument("--workers", type=int, required=True)
    w2v.add_argument("--reps", type=int, default=2)
    w2v.add_argument("--scratch", required=True, help="where checkpoints are written, then deleted")
    w2v.add_argument("--label", required=True)
    bert = sub.add_parser("bert")
    bert.add_argument("--pairs", required=True, help="candidates_w<ordinal>.csv")
    bert.add_argument("--config", required=True, help="config.json of a saved state")
    bert.add_argument("--device", required=True, help="cuda, or cpu:<cores>")
    bert.add_argument("--reps", type=int, default=2)
    bert.add_argument("--label", required=True)
    args = parser.parse_args()
    (replay_word2vec if args.mode == "word2vec" else replay_bert)(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
