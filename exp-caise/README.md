# CAiSE experiments

Pipeline variants compared by energy and matching quality. The pipeline code lives in
`code/Energy-Aware-Entity-Resolution`; this folder only holds the scripts that run it.

## TF-IDF + BERT demo

`run_demo.py` runs one stream per dataset in a single process, with the pipeline's functions
and no Kubernetes, Kafka or embedding stages:

1. TF-IDF index of table A (`pipeline/tfidf_blocking.py`)
2. BERT trained on the dataset's `train.csv` / `valid.csv` (`pipeline/bert_training.py`)
3. table B in windows: running-best TF-IDF candidates
   (`score_running_best_tfidf_pairs`), judged by BERT, then the capacity-constrained
   selection (`select_matches`)
4. candidate recall, precision / recall / F1, and time per stage

Datasets and their settings (window, `k`, `query_top_k`, cardinality, TF-IDF fields) are in
`datasets.yaml`.

On server1 (GPU), from the repository root:

```bash
bash exp-caise/run_demo.sh setup                       # once
DATA_ROOT=/data bash exp-caise/run_demo.sh check       # files of every dataset in place?
nohup bash exp-caise/run_demo.sh run > caise-demo.log 2>&1 &
# a subset, another threshold, or candidates only:
bash exp-caise/run_demo.sh run --datasets abt_buy,movie --threshold 0.7
bash exp-caise/run_demo.sh run --no-bert
```

`DATA_ROOT` (default `/data`) must hold `exp_datasets/` and `rtas/retrieval-extrait-hn/`.
Results: `reports/caise-demo-<time>/summary.csv`, and per dataset `result.json`,
`candidates.csv` (every candidate with TF-IDF score, BERT probability, window and label) and
`windows.jsonl`. A trained BERT model is reused when the same `OUTPUT` is run again.

Caveats:

- `best_f1_on_truth` is the F1 of the best threshold chosen on the ground truth: an upper
  reference, not a result. Report `f1` at a threshold fixed in advance (or chosen on
  `valid.csv`).
- BERT's training pairs come from the same ground truth that evaluates the output (as in the
  pipeline). retrieval-extrait-hn holds the training pairs' B records out of the stream.
- The demo measures time, not energy.

## Candidate recall of TF-IDF (no BERT, laptop, `max_df` 0.1)

| dataset | candidate recall | pairs / window |
|---|---|---|
| fodors_zagats | 1.000 | 1020 |
| movie | 0.999 | 447 |
| dblp_acm / dirty | 0.996 / 0.998 | ~1860 |
| dblp_scholar / dirty | 0.996 / 0.996 | ~1430 |
| walmart_amazon / dirty | 0.997 / 0.994 | ~1540 |
| amazon_google | 0.982 | 1633 |
| beer | 0.971 | 2959 |
| itunes_amazon / dirty (`k` 4) | 0.917 / 0.917 | ~1620 |
| abt_buy | 0.910 | 1737 |
