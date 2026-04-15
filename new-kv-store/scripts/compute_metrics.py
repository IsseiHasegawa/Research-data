#!/usr/bin/env python3
"""
compute_metrics.py — Compute per-run and aggregate experiment metrics.

Reads logs from output/logs/<config>/<trial>/ directories.
Computes:
  - Detection latency (time from fault injection to declared_dead)
  - Downtime (time from fault until first successful client op)
  - Write latency (per-op latency from workload logs: median, p95, p99)
  - Throughput (successful ops / time window)
  - False positives (declared_dead events without actual fault injection)
  - Data loss risk (SETs acknowledged by primary but not replicated before crash)

Outputs:
  - output/results/summary.csv (per-trial metrics)
  - output/results/aggregate.csv (per-config aggregated with median, IQR)
  - output/results/heatmap.csv (for heatmap plotting)
  - output/results/scatter.csv (for scatter plotting)
"""

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

# Allow importing parse_logs from same directory
sys.path.insert(0, str(Path(__file__).resolve().parent))
from parse_logs import load_trial_logs, filter_events, get_first_event


def compute_trial_metrics(trial_dir: Path) -> Dict[str, Any]:
    """Compute all metrics for a single trial."""
    logs = load_trial_logs(trial_dir)
    injector = logs["injector"]
    node0 = logs["node0"]
    workload = logs["workload"]

    metrics: Dict[str, Any] = {
        "trial_dir": str(trial_dir),
    }

    # ── Extract config from injector ──────────────────────────────────────────
    run_start = get_first_event(injector, "run_start")
    if run_start:
        metrics["config"]             = run_start.get("config", "")
        metrics["hb_interval_ms"]     = run_start.get("hb_interval_ms", 0)
        metrics["hb_timeout_ms"]      = run_start.get("hb_timeout_ms", 0)
        metrics["repl_mode"]          = run_start.get("repl_mode", "")
        metrics["fault_type"]         = run_start.get("fault_type", "")
        metrics["run_id"]             = run_start.get("run_id", "")
        metrics["fd_algo"]            = run_start.get("fd_algo", "fixed")      # NEW
        metrics["phi_threshold"]      = run_start.get("phi_threshold", None)   # NEW
        metrics["workload_num_clients"]= run_start.get("workload_num_clients", 1)  # NEW
        metrics["workload_zipf_alpha"]= run_start.get("workload_zipf_alpha", 0.0) # NEW

    # ── Detection latency ─────────────────────────────────────────────────────
    detection_result = get_first_event(injector, "detection_result")
    if detection_result:
        metrics["t_fault_ms"] = detection_result.get("t_fault_ms")
        metrics["t_detect_ms"] = detection_result.get("t_detect_ms")
        metrics["detection_latency_ms"] = detection_result.get("detection_latency_ms")
    else:
        metrics["t_fault_ms"] = None
        metrics["t_detect_ms"] = None
        metrics["detection_latency_ms"] = None

    # ── Workload metrics ──────────────────────────────────────────────────────
    op_dones = filter_events(workload, "op_done")
    if op_dones:
        latencies_us = [e["latency_us"] for e in op_dones
                        if e.get("success") and "latency_us" in e]
        success_ops = [e for e in op_dones if e.get("success")]
        failed_ops = [e for e in op_dones if not e.get("success")]

        if latencies_us:
            arr = np.array(latencies_us, dtype=float)
            metrics["write_latency_median_us"] = float(np.median(arr))
            metrics["write_latency_p95_us"] = float(np.percentile(arr, 95))
            metrics["write_latency_p99_us"] = float(np.percentile(arr, 99))
            metrics["write_latency_mean_us"] = float(np.mean(arr))
        else:
            metrics["write_latency_median_us"] = None
            metrics["write_latency_p95_us"] = None
            metrics["write_latency_p99_us"] = None
            metrics["write_latency_mean_us"] = None

        metrics["total_ops"] = len(op_dones)
        metrics["success_ops"] = len(success_ops)
        metrics["failed_ops"] = len(failed_ops)

        # Throughput: successful ops / duration
        if len(success_ops) >= 2:
            ts_min = min(e["ts_ms"] for e in success_ops)
            ts_max = max(e["ts_ms"] for e in success_ops)
            duration_sec = (ts_max - ts_min) / 1000.0
            if duration_sec > 0:
                metrics["throughput_ops_sec"] = len(success_ops) / duration_sec
            else:
                metrics["throughput_ops_sec"] = float(len(success_ops))
        else:
            metrics["throughput_ops_sec"] = 0.0
    else:
        metrics["write_latency_median_us"] = None
        metrics["write_latency_p95_us"] = None
        metrics["write_latency_p99_us"] = None
        metrics["write_latency_mean_us"] = None
        metrics["total_ops"] = 0
        metrics["success_ops"] = 0
        metrics["failed_ops"] = 0
        metrics["throughput_ops_sec"] = 0.0

    # ── Downtime ──────────────────────────────────────────────────────────────
    # Time from fault until first successful op after fault
    t_fault = metrics.get("t_fault_ms")
    if t_fault is not None and op_dones:
        post_fault_success = [
            e for e in op_dones
            if e.get("success") and e.get("ts_ms", 0) >= t_fault
        ]
        if post_fault_success:
            first_success_after_fault = min(e["ts_ms"] for e in post_fault_success)
            metrics["downtime_ms"] = first_success_after_fault - t_fault
        else:
            metrics["downtime_ms"] = None  # No successful ops after fault
    else:
        metrics["downtime_ms"] = None

    # ── False positives ───────────────────────────────────────────────────────
    # declared_dead events in node0 that happen before any fault injection
    declared_deads = filter_events(node0, "declared_dead")
    fault_inject = get_first_event(injector, "fault_inject")
    t_inject = fault_inject.get("ts_ms", float("inf")) if fault_inject else float("inf")
    false_positives = [d for d in declared_deads if d.get("ts_ms", 0) < t_inject - 500]
    metrics["false_positives"] = len(false_positives)

    # ── Data loss risk ────────────────────────────────────────────────────────
    # Count repl_skipped events (writes that couldn't be replicated)
    repl_skipped = filter_events(node0, "repl_skipped") if node0 else []
    metrics["repl_skipped_count"] = len(repl_skipped)

    # ── Stale read detection (read-your-writes violations) ────────────────────
    stale_read_events = filter_events(workload, "stale_read")
    metrics["stale_read_count"] = len(stale_read_events)
    total_gets = len([e for e in filter_events(workload, "op_done")
                      if e.get("op_type") == "GET"])
    metrics["stale_read_rate"] = (
        metrics["stale_read_count"] / total_gets if total_gets > 0 else 0.0
    )  # fraction of GETs that were stale

    # ── WAL recovery info ─────────────────────────────────────────────────────
    wal_recovered = get_first_event(node0, "wal_recovered") if node0 else None
    metrics["wal_recovered_entries"] = (
        wal_recovered.get("extra", {}).get("entries", 0)
        if isinstance(wal_recovered, dict) else 0
    )

    # ── Hot-key latency split (Zipf experiments) ──────────────────────────────
    # "hot" = key_0..key_4 (top 10% of 50-key space); "cold" = rest
    op_dones = filter_events(workload, "op_done")
    hot_lat, cold_lat = [], []
    for e in op_dones:
        if not e.get("success") or "latency_us" not in e:
            continue
        key = e.get("key", "")
        try:
            idx = int(key.split("_")[-1])
        except ValueError:
            continue
        (hot_lat if idx < 5 else cold_lat).append(e["latency_us"])

    if hot_lat:
        metrics["hot_key_latency_median_us"] = float(np.median(hot_lat))
    else:
        metrics["hot_key_latency_median_us"] = None
    if cold_lat:
        metrics["cold_key_latency_median_us"] = float(np.median(cold_lat))
    else:
        metrics["cold_key_latency_median_us"] = None

    return metrics


def aggregate_metrics(all_metrics: List[Dict]) -> List[Dict]:
    """
    Aggregate trial metrics by configuration.
    Computes median and IQR for numeric metrics.
    """
    by_config: Dict[str, List[Dict]] = defaultdict(list)
    for m in all_metrics:
        cfg = m.get("config", "unknown")
        by_config[cfg].append(m)

    aggregated = []
    for config_name, trials in sorted(by_config.items()):
        row = {
            "config": config_name,
            "n_trials": len(trials),
            "hb_interval_ms": trials[0].get("hb_interval_ms", 0),
            "hb_timeout_ms": trials[0].get("hb_timeout_ms", 0),
            "repl_mode": trials[0].get("repl_mode", ""),
            "fault_type": trials[0].get("fault_type", ""),
        }

        # Aggregate numeric fields
        numeric_fields = [
            "detection_latency_ms", "downtime_ms",
            "write_latency_median_us", "write_latency_p95_us",
            "throughput_ops_sec", "false_positives", "repl_skipped_count",
            "stale_read_count", "stale_read_rate",          # NEW
            "hot_key_latency_median_us", "cold_key_latency_median_us",  # NEW
        ]
        for field in numeric_fields:
            values = [m[field] for m in trials if m.get(field) is not None]
            if values:
                arr = np.array(values, dtype=float)
                row[f"{field}_median"] = float(np.median(arr))
                q1, q3 = np.percentile(arr, [25, 75])
                row[f"{field}_iqr"] = float(q3 - q1)
                row[f"{field}_min"] = float(np.min(arr))
                row[f"{field}_max"] = float(np.max(arr))
            else:
                row[f"{field}_median"] = None
                row[f"{field}_iqr"] = None
                row[f"{field}_min"] = None
                row[f"{field}_max"] = None

        # Missed heartbeats ratio
        hb_i = row["hb_interval_ms"]
        hb_t = row["hb_timeout_ms"]
        row["missed"] = hb_t / hb_i if hb_i > 0 else None

        # Copy categorical fields from first trial
        row["fd_algo"] = trials[0].get("fd_algo", "fixed")          # NEW
        row["phi_threshold"] = trials[0].get("phi_threshold", None) # NEW
        row["workload_zipf_alpha"] = trials[0].get("workload_zipf_alpha", 0.0)  # NEW

        aggregated.append(row)

    return aggregated


def write_summary_csv(metrics: List[Dict], path: Path):
    """Write per-trial metrics to CSV."""
    if not metrics:
        return
    fieldnames = [
        "config", "run_id", "hb_interval_ms", "hb_timeout_ms", "repl_mode",
        "fault_type", "detection_latency_ms", "downtime_ms",
        "write_latency_median_us", "write_latency_p95_us", "write_latency_p99_us",
        "throughput_ops_sec", "total_ops", "success_ops", "failed_ops",
        "false_positives", "repl_skipped_count",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for m in metrics:
            writer.writerow(m)
    print(f"Wrote {path}")


def write_aggregate_csv(aggregated: List[Dict], path: Path):
    """Write aggregated metrics to CSV."""
    if not aggregated:
        return
    fieldnames = list(aggregated[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in aggregated:
            writer.writerow(row)
    print(f"Wrote {path}")


def write_heatmap_csv(aggregated: List[Dict], path: Path):
    """Write heatmap-format CSV for detection latency visualization."""
    rows = [r for r in aggregated if r.get("detection_latency_ms_median") is not None]
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["hb_timeout_ms", "hb_interval_ms", "repl_mode",
                          "median_detection_ms", "iqr_detection_ms", "n_trials"])
        for r in sorted(rows, key=lambda x: (x["hb_timeout_ms"], x["hb_interval_ms"])):
            writer.writerow([
                r["hb_timeout_ms"], r["hb_interval_ms"], r["repl_mode"],
                r["detection_latency_ms_median"],
                r["detection_latency_ms_iqr"],
                r["n_trials"],
            ])
    print(f"Wrote {path}")


def write_scatter_csv(aggregated: List[Dict], path: Path):
    """Write scatter-format CSV for missed-heartbeats vs detection latency."""
    rows = [r for r in aggregated
            if r.get("missed") is not None and r.get("detection_latency_ms_median") is not None]
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["missed", "hb_interval_ms", "hb_timeout_ms", "repl_mode",
                          "median_detection_ms", "iqr_detection_ms", "n_trials"])
        for r in sorted(rows, key=lambda x: x["missed"]):
            writer.writerow([
                r["missed"], r["hb_interval_ms"], r["hb_timeout_ms"], r["repl_mode"],
                r["detection_latency_ms_median"],
                r["detection_latency_ms_iqr"],
                r["n_trials"],
            ])
    print(f"Wrote {path}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Compute experiment metrics")
    parser.add_argument("--output", type=Path,
                        default=Path(__file__).resolve().parent.parent / "output",
                        help="Output directory (contains logs/ and results/)")
    args = parser.parse_args()

    logs_dir = args.output / "logs"
    results_dir = args.output / "results"

    if not logs_dir.exists():
        print(f"No logs directory at {logs_dir}", file=sys.stderr)
        sys.exit(1)

    # Discover all trial directories
    all_metrics = []
    for config_dir in sorted(logs_dir.iterdir()):
        if not config_dir.is_dir():
            continue
        for trial_dir in sorted(config_dir.iterdir()):
            if not trial_dir.is_dir():
                continue
            try:
                metrics = compute_trial_metrics(trial_dir)
                all_metrics.append(metrics)
            except Exception as e:
                print(f"  Error processing {trial_dir}: {e}", file=sys.stderr)

    if not all_metrics:
        print("No trial data found.", file=sys.stderr)
        sys.exit(1)

    print(f"Processed {len(all_metrics)} trials")

    # Write per-trial summary
    write_summary_csv(all_metrics, results_dir / "summary.csv")

    # Aggregate by config
    aggregated = aggregate_metrics(all_metrics)
    write_aggregate_csv(aggregated, results_dir / "aggregate.csv")

    # Specialized CSVs for plotting
    write_heatmap_csv(aggregated, results_dir / "heatmap.csv")
    write_scatter_csv(aggregated, results_dir / "scatter.csv")


if __name__ == "__main__":
    main()
