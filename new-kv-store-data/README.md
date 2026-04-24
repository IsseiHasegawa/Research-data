# Research-data / new-kv-store-data

This repository runs experiments, computes metrics, and generates plots using binaries built in `Key-Value-stores`.  
The goal of this README is to let someone go from clone to reproducible data collection.

## Basic Information

- Role: Data collection and analysis repository
- Language: Python 3 (targets C++ binaries)
- Main scripts:
  - `scripts/run_experiments.py` (experiment orchestration)
  - `scripts/compute_metrics.py` (metric computation)
  - `scripts/plot_results.py` (visualization)
- Main outputs:
  - `output/logs/` (raw logs)
  - `output/results/*.csv` (computed metrics)
  - `output/plots/*.png` (figures)

## Folder Structure

```text
new-kv-store-data/
├── README.md
├── requirements.txt
├── config/
│   └── experiment_configs.json      # experiment parameters (HB, replication, faults, trials)
├── scripts/
│   ├── run_experiments.py           # start nodes, inject faults, save logs
│   ├── parse_logs.py                # log parsing helpers
│   ├── compute_metrics.py           # writes summary/aggregate/heatmap/scatter CSVs
│   ├── plot_results.py              # standard plots
│   └── plot_paper_figures.py        # paper-ready plots
└── output/                          # created/updated at runtime
    ├── logs/
    ├── results/
    └── plots/
```

## Prerequisites

- Python 3.10+
- `Key-Value-stores` cloned under the same parent directory and built
- `Key-Value-stores/build/kvnode` and `Key-Value-stores/build/kv_workload` available

## From Clone to Data Collection (Recommended Path)

```bash
cd <workspace>
git clone <impl-repo-url> Key-Value-stores
git clone <data-repo-url> Research-data
```

### 1) Build the implementation repository

```bash
cd <workspace>/Key-Value-stores
mkdir -p build
cd build
cmake ..
make
```

### 2) Install Python dependencies

```bash
cd <workspace>/Research-data/new-kv-store-data
python3 -m pip install -r requirements.txt
```

### 3) Run a quick smoke test (small trial count)

```bash
python3 scripts/run_experiments.py \
  --impl-root ../../Key-Value-stores \
  --filter fast \
  --trials 2
```

### 4) Run full data collection

```bash
python3 scripts/run_experiments.py \
  --impl-root ../../Key-Value-stores
```

## Collected Outputs

- Per-trial metrics:
  - `output/results/summary.csv`
- Per-configuration aggregates (main comparison table):
  - `output/results/aggregate.csv`
- Plot-oriented tables:
  - `output/results/heatmap.csv`
  - `output/results/scatter.csv`
- Detailed logs:
  - `output/logs/<config>/trial_<n>/node0.jsonl`
  - `output/logs/<config>/trial_<n>/node1.jsonl`
  - `output/logs/<config>/trial_<n>/workload.jsonl`
  - `output/logs/<config>/trial_<n>/injector.jsonl`

## Main Metrics

Key metrics for comparison:

- `detection_latency_ms`
- `downtime_ms`
- `write_latency_median_us`, `write_latency_p95_us`, `write_latency_p99_us`
- `throughput_ops_sec`
- `false_positives`
- `repl_skipped_count`

## Recompute Metrics / Regenerate Plots

```bash
python3 scripts/compute_metrics.py --output output
python3 scripts/plot_results.py --output output
```

## Troubleshooting

- `kv binaries not found`:
  - Check the `--impl-root` path
  - Verify `Key-Value-stores/build/kvnode` and `Key-Value-stores/build/kv_workload`
- Port conflicts:
  - Stop existing `kvnode` processes, then rerun
- Need faster iterations:
  - Use `--filter` and `--trials` to reduce scope
