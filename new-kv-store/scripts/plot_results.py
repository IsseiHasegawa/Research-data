#!/usr/bin/env python3
"""
plot_results.py — Generate publication-quality visualizations from experiment results.

Reads CSV files produced by compute_metrics.py and generates:
  1. Heatmap: detection latency vs (hb_interval, hb_timeout)
  2. Scatter: detection latency vs missed heartbeats
  3. Bar chart: write latency by replication mode
  4. Bar chart: throughput by replication mode
  5. Grouped bar: metrics comparison across fault types
  6. Line graph: detection latency vs heartbeat interval (grouped by repl mode)
  7. Box plot: per-trial detection latency distribution

Outputs high-resolution PNGs to output/plots/.
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np


# ── Publication-quality style configuration ────────────────────────────────────
STYLE = {
    # Typography — use LaTeX-compatible serif font families
    "font.family": "serif",
    "font.serif": ["Times New Roman", "DejaVu Serif", "Computer Modern Roman"],
    "mathtext.fontset": "cm",
    "text.usetex": False,           # set True if LaTeX is installed
    # Font sizes (ACM/IEEE typical values)
    "font.size": 10,
    "axes.titlesize": 11,
    "axes.labelsize": 10,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 9,
    "legend.title_fontsize": 9,
    # Figure
    "figure.facecolor": "white",
    "figure.dpi": 100,
    # Axes
    "axes.facecolor": "white",
    "axes.edgecolor": "#333333",
    "axes.linewidth": 0.8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    # Grid — subtle horizontal only
    "axes.grid": True,
    "axes.grid.axis": "y",
    "grid.color": "#cccccc",
    "grid.linewidth": 0.5,
    "grid.linestyle": "--",
    "grid.alpha": 0.7,
    # Ticks
    "xtick.direction": "out",
    "ytick.direction": "out",
    "xtick.major.width": 0.8,
    "ytick.major.width": 0.8,
    "xtick.major.size": 4,
    "ytick.major.size": 4,
    # Legend
    "legend.frameon": True,
    "legend.framealpha": 0.9,
    "legend.edgecolor": "#cccccc",
    # Lines / markers
    "lines.linewidth": 1.5,
    "lines.markersize": 6,
    # Error bars
    "errorbar.capsize": 4,
    # Save
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.facecolor": "white",
    "savefig.pad_inches": 0.05,
}

# ── Color palette (colorblind-safe, print-friendly) ────────────────────────────
# Based on Wong (2011) colorblind-safe palette
PALETTE = {
    "none":  "#0072B2",   # blue
    "sync":  "#D55E00",   # vermillion
    "async": "#009E73",   # green
    "default": "#CC79A7", # pink
}

# Hatching patterns for black-and-white printing
HATCHES = {
    "none":  "",
    "sync":  "///",
    "async": "...",
}

MARKER_STYLES = {
    "none":  "o",
    "sync":  "s",
    "async": "^",
}


def load_csv(path: Path) -> list[dict]:
    """Load a CSV file into a list of dicts."""
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def safe_float(val, default=None):
    """Convert to float safely."""
    if val is None or val == "" or val == "None":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def format_config_label(label: str) -> str:
    """
    Transform internal config names into human-readable labels for axes.
    e.g. 'fd_fast_async_repl' -> 'FD-fast\\nAsync'
    """
    replacements = {
        "fd_fast_async_repl": "FD Fast\nAsync Repl.",
        "fd_fast_no_repl":    "FD Fast\nNo Repl.",
        "fd_fast_sync_repl":  "FD Fast\nSync Repl.",
        "fd_slow_async_repl": "FD Slow\nAsync Repl.",
        "fd_slow_no_repl":    "FD Slow\nNo Repl.",
        "fd_slow_sync_repl":  "FD Slow\nSync Repl.",
        "quick_no_repl":      "Quick\nNo Repl.",
        "quick_sync_repl":    "Quick\nSync Repl.",
        "quick_async_repl":   "Quick\nAsync Repl.",
        "delay_fast_sync":    "Delay Fast\nSync",
        "delay_slow_async":   "Delay Slow\nAsync",
    }
    return replacements.get(label, label.replace("_", "\n"))


def format_repl_label(mode: str) -> str:
    """Return display name for replication mode."""
    return {"none": "None", "sync": "Synchronous", "async": "Asynchronous"}.get(mode, mode.title())


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Plot experiment results (publication quality)")
    parser.add_argument("--output", type=Path,
                        default=Path(__file__).resolve().parent.parent / "output",
                        help="Output directory (contains results/)")
    args = parser.parse_args()

    results_dir = args.output / "results"
    plots_dir = args.output / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        from matplotlib.colors import Normalize
        import matplotlib.ticker as ticker
    except ImportError:
        print("Error: matplotlib not found. Install: pip install matplotlib", file=sys.stderr)
        sys.exit(1)

    plt.rcParams.update(STYLE)

    # ── Load data ──────────────────────────────────────────────────────────────
    summary     = load_csv(results_dir / "summary.csv")
    aggregate   = load_csv(results_dir / "aggregate.csv")
    heatmap_data = load_csv(results_dir / "heatmap.csv")
    scatter_data = load_csv(results_dir / "scatter.csv")

    plots_generated = 0

    # ── 1. Scatter: missed heartbeats vs detection latency ────────────────────
    if scatter_data:
        missed     = [safe_float(r["missed"]) for r in scatter_data]
        median_det = [safe_float(r["median_detection_ms"]) for r in scatter_data]
        iqr_det    = [safe_float(r["iqr_detection_ms"], 0) for r in scatter_data]
        labels     = [r.get("repl_mode", "") for r in scatter_data]

        if all(v is not None for v in missed) and all(v is not None for v in median_det):
            fig, ax = plt.subplots(figsize=(5.5, 3.8))

            colors  = [PALETTE.get(l, PALETTE["default"]) for l in labels]
            markers = [MARKER_STYLES.get(l, "o") for l in labels]

            # Draw error bars first (behind markers)
            ax.errorbar(missed, median_det,
                        yerr=[i / 2 if i else 0 for i in iqr_det],
                        fmt="none",
                        ecolor="#888888",
                        capsize=3,
                        capthick=0.8,
                        elinewidth=0.8,
                        zorder=1)

            # Plot each point with its own marker shape for print readability
            for x, y, c, m in zip(missed, median_det, colors, markers):
                ax.scatter(x, y, c=c, marker=m, s=55,
                           edgecolors="white", linewidths=0.6, zorder=2)

            # Manual legend entries
            handles = [
                mpatches.Patch(facecolor=PALETTE[mode], label=format_repl_label(mode),
                               edgecolor="#333333", linewidth=0.5)
                for mode in ["none", "sync", "async"]
                if mode in set(labels)
            ]
            ax.legend(handles=handles,
                      title="Replication Mode",
                      loc="upper left",
                      handlelength=1.2,
                      handleheight=1.0)

            ax.set_xlabel("Missed Heartbeats (timeout / interval)")
            ax.set_ylabel("Median Detection Latency (ms)")
            ax.set_title("Failure Detection Latency vs. Missed Heartbeats",
                         pad=8)

            # Ensure x-axis shows integer ticks when appropriate
            ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))

            fig.tight_layout()
            fig.savefig(plots_dir / "scatter_detection_latency.png")
            plt.close(fig)
            plots_generated += 1
            print("Saved scatter_detection_latency.png")

    # ── 2. Heatmap: detection latency by (hb_interval, hb_timeout) ───────────
    if heatmap_data:
        timeouts  = sorted({safe_float(r["hb_timeout_ms"])  for r in heatmap_data})
        intervals = sorted({safe_float(r["hb_interval_ms"]) for r in heatmap_data})
        timeouts  = [t for t in timeouts  if t is not None]
        intervals = [i for i in intervals if i is not None]

        if len(timeouts) > 1 or len(intervals) > 1:
            Z = np.full((len(timeouts), len(intervals)), np.nan)
            for r in heatmap_data:
                to_val  = safe_float(r["hb_timeout_ms"])
                iv_val  = safe_float(r["hb_interval_ms"])
                med_val = safe_float(r["median_detection_ms"])
                if to_val is not None and iv_val is not None and med_val is not None:
                    ti = timeouts.index(to_val)
                    ii = intervals.index(iv_val)
                    Z[ti, ii] = med_val if np.isnan(Z[ti, ii]) else (Z[ti, ii] + med_val) / 2

            fig, ax = plt.subplots(figsize=(5.0, 4.0))
            # Use a perceptually uniform sequential colormap
            im = ax.imshow(Z, aspect="auto", cmap="viridis", origin="lower",
                           interpolation="nearest")

            ax.set_xticks(range(len(intervals)))
            ax.set_yticks(range(len(timeouts)))
            ax.set_xticklabels([int(x) for x in intervals])
            ax.set_yticklabels([int(x) for x in timeouts])
            ax.set_xlabel("Heartbeat Interval (ms)")
            ax.set_ylabel("Heartbeat Timeout (ms)")
            ax.set_title("Median Failure Detection Latency (ms)", pad=8)

            # Turn off heatmap-specific grid
            ax.grid(False)
            ax.tick_params(left=False, bottom=False)

            # Annotate cells
            vmin, vmax = np.nanmin(Z), np.nanmax(Z)
            midpoint   = (vmin + vmax) / 2
            for i in range(len(timeouts)):
                for j in range(len(intervals)):
                    v = Z[i, j]
                    if not np.isnan(v):
                        text_color = "white" if v > midpoint else "black"
                        ax.text(j, i, f"{v:.0f}",
                                ha="center", va="center",
                                color=text_color,
                                fontsize=10,
                                fontweight="bold")

            cbar = plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
            cbar.set_label("Detection Latency (ms)", labelpad=6)
            cbar.ax.tick_params(labelsize=8)

            fig.tight_layout()
            fig.savefig(plots_dir / "heatmap_detection_latency.png")
            plt.close(fig)
            plots_generated += 1
            print("Saved heatmap_detection_latency.png")

    # ── 3. Bar chart: write latency by replication mode ───────────────────────
    if aggregate:
        by_repl: dict[str, list] = defaultdict(list)
        for r in aggregate:
            mode = r.get("repl_mode", "unknown")
            med  = safe_float(r.get("write_latency_median_us_median"))
            if med is not None:
                by_repl[mode].append(med)

        if by_repl:
            order  = [m for m in ["none", "sync", "async"] if m in by_repl]
            order += [m for m in sorted(by_repl) if m not in order]
            means  = [np.mean(by_repl[m]) for m in order]
            stds   = [np.std(by_repl[m]) if len(by_repl[m]) > 1 else 0 for m in order]
            bar_colors  = [PALETTE.get(m, PALETTE["default"]) for m in order]
            bar_hatches = [HATCHES.get(m, "") for m in order]
            x_labels    = [format_repl_label(m) for m in order]

            fig, ax = plt.subplots(figsize=(4.5, 3.5))
            bars = ax.bar(range(len(order)), means, yerr=stds,
                          color=bar_colors,
                          hatch=bar_hatches,
                          edgecolor="#333333",
                          linewidth=0.7,
                          capsize=3,
                          error_kw={"elinewidth": 0.8, "ecolor": "#444444"},
                          alpha=0.88,
                          width=0.55)

            ax.set_xticks(range(len(order)))
            ax.set_xticklabels(x_labels)
            ax.set_xlabel("Replication Mode")
            ax.set_ylabel("Median Write Latency (\u03bcs)")
            ax.set_title("Write Latency by Replication Mode", pad=8)

            # Value annotations above error bar tops
            y_pad = max(means) * 0.04
            for bar, val, std in zip(bars, means, stds):
                top = val + std + y_pad   # above the error bar cap
                ax.text(bar.get_x() + bar.get_width() / 2,
                        top,
                        f"{val:.0f}",
                        ha="center", va="bottom",
                        fontsize=8.5, color="#222222",
                        bbox=dict(boxstyle="round,pad=0.15",
                                  facecolor="white", edgecolor="none",
                                  alpha=0.75))

            ax.set_ylim(0, max(v + s for v, s in zip(means, stds)) * 1.30)
            ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=6, integer=True))
            fig.tight_layout()
            fig.savefig(plots_dir / "write_latency_by_repl.png")
            plt.close(fig)
            plots_generated += 1
            print("Saved write_latency_by_repl.png")

    # ── 4. Bar chart: throughput by replication mode ──────────────────────────
    if aggregate:
        by_repl_tp: dict[str, list] = defaultdict(list)
        for r in aggregate:
            mode = r.get("repl_mode", "unknown")
            tp   = safe_float(r.get("throughput_ops_sec_median"))
            if tp is not None:
                by_repl_tp[mode].append(tp)

        if by_repl_tp:
            order  = [m for m in ["none", "sync", "async"] if m in by_repl_tp]
            order += [m for m in sorted(by_repl_tp) if m not in order]
            means  = [np.mean(by_repl_tp[m]) for m in order]
            stds   = [np.std(by_repl_tp[m]) if len(by_repl_tp[m]) > 1 else 0 for m in order]
            bar_colors  = [PALETTE.get(m, PALETTE["default"]) for m in order]
            bar_hatches = [HATCHES.get(m, "") for m in order]
            x_labels    = [format_repl_label(m) for m in order]

            fig, ax = plt.subplots(figsize=(4.5, 3.5))
            bars = ax.bar(range(len(order)), means, yerr=stds,
                          color=bar_colors,
                          hatch=bar_hatches,
                          edgecolor="#333333",
                          linewidth=0.7,
                          capsize=3,
                          error_kw={"elinewidth": 0.8, "ecolor": "#444444"},
                          alpha=0.88,
                          width=0.55)

            ax.set_xticks(range(len(order)))
            ax.set_xticklabels(x_labels)
            ax.set_xlabel("Replication Mode")
            ax.set_ylabel("Throughput (ops/s)")
            ax.set_title("Throughput by Replication Mode", pad=8)

            y_pad = max(means) * 0.04
            for bar, val, std in zip(bars, means, stds):
                top = val + std + y_pad   # above the error bar cap
                ax.text(bar.get_x() + bar.get_width() / 2,
                        top,
                        f"{val:.0f}",
                        ha="center", va="bottom",
                        fontsize=8.5, color="#222222",
                        bbox=dict(boxstyle="round,pad=0.15",
                                  facecolor="white", edgecolor="none",
                                  alpha=0.75))

            ax.set_ylim(0, max(v + s for v, s in zip(means, stds)) * 1.30)
            ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=6, integer=True))
            fig.tight_layout()
            fig.savefig(plots_dir / "throughput_by_repl.png")
            plt.close(fig)
            plots_generated += 1
            print("Saved throughput_by_repl.png")

    # ── 5. Grouped comparison: detection latency + downtime by config ─────────
    if aggregate:
        configs  = [r.get("config", "") for r in aggregate]
        det_lat  = [safe_float(r.get("detection_latency_ms_median"), 0) for r in aggregate]
        downtime = [safe_float(r.get("downtime_ms_median"), 0) for r in aggregate]

        if any(d > 0 for d in det_lat):
            n       = len(configs)
            width   = 0.38
            x       = np.arange(n)
            fig_w   = max(6.5, n * 0.9)
            fig, ax = plt.subplots(figsize=(fig_w, 4.0))

            bars1 = ax.bar(x - width / 2, det_lat, width,
                           label="Detection Latency",
                           color=PALETTE["none"], alpha=0.88,
                           edgecolor="#333333", linewidth=0.7)
            bars2 = ax.bar(x + width / 2, downtime, width,
                           label="Downtime",
                           color=PALETTE["sync"], alpha=0.88,
                           edgecolor="#333333", linewidth=0.7,
                           hatch="///")

            ax.set_xlabel("Experimental Configuration")
            ax.set_ylabel("Time (ms)")
            ax.set_title("Detection Latency and Downtime by Configuration", pad=8)
            ax.set_xticks(x)
            ax.set_xticklabels(
                [format_config_label(c) for c in configs],
                rotation=0, ha="center", fontsize=7.5
            )
            ax.legend(frameon=True, loc="upper left")
            ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=6, integer=True))

            fig.tight_layout()
            fig.savefig(plots_dir / "detection_vs_downtime.png")
            plt.close(fig)
            plots_generated += 1
            print("Saved detection_vs_downtime.png")

    # ── 6. Line: detection latency vs hb_interval, grouped by repl_mode ──────
    if aggregate:
        by_repl_line: dict[str, dict] = defaultdict(lambda: {"intervals": [], "latencies": []})
        for r in aggregate:
            mode  = r.get("repl_mode", "unknown")
            hb_i  = safe_float(r.get("hb_interval_ms"))
            lat   = safe_float(r.get("detection_latency_ms_median"))
            if hb_i is not None and lat is not None:
                by_repl_line[mode]["intervals"].append(hb_i)
                by_repl_line[mode]["latencies"].append(lat)

        if by_repl_line:
            fig, ax = plt.subplots(figsize=(5.5, 3.8))

            for mode, data in sorted(by_repl_line.items()):
                pairs = sorted(zip(data["intervals"], data["latencies"]))
                if not pairs:
                    continue
                ivs, lats = zip(*pairs)
                ax.plot(ivs, lats,
                        marker=MARKER_STYLES.get(mode, "o"),
                        color=PALETTE.get(mode, PALETTE["default"]),
                        linewidth=1.5,
                        markersize=5.5,
                        markeredgecolor="white",
                        markeredgewidth=0.5,
                        label=format_repl_label(mode))

            ax.set_xlabel("Heartbeat Interval (ms)")
            ax.set_ylabel("Median Detection Latency (ms)")
            ax.set_title("Detection Latency vs. Heartbeat Interval", pad=8)
            ax.legend(title="Replication Mode", loc="upper left")
            ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
            ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=6, integer=True))

            fig.tight_layout()
            fig.savefig(plots_dir / "detection_vs_interval.png")
            plt.close(fig)
            plots_generated += 1
            print("Saved detection_vs_interval.png")

    # ── 7. Per-trial latency distribution (box plot) ──────────────────────────
    if summary:
        by_config_latency: dict[str, list] = defaultdict(list)
        for r in summary:
            cfg = r.get("config", "unknown")
            lat = safe_float(r.get("detection_latency_ms"))
            if lat is not None and lat >= 0:
                by_config_latency[cfg].append(lat)

        if by_config_latency:
            configs  = sorted(by_config_latency.keys())
            data     = [by_config_latency[c] for c in configs]
            n_groups = len(configs)
            fig_w    = max(6.5, n_groups * 0.9)

            fig, ax = plt.subplots(figsize=(fig_w, 4.0))

            bp = ax.boxplot(
                data,
                patch_artist=True,
                notch=True,
                widths=0.45,
                medianprops={"color": "#D55E00", "linewidth": 1.5},
                whiskerprops={"linewidth": 0.9, "color": "#444444"},
                capprops={"linewidth": 0.9, "color": "#444444"},
                flierprops={"marker": "o", "markersize": 3,
                             "markerfacecolor": "#888888",
                             "markeredgecolor": "none", "alpha": 0.6},
                boxprops={"linewidth": 0.8},
            )

            # Alternate fill colors for adjacent boxes
            fill_colors = ["#0072B2", "#56B4E9"]  # two blue shades
            for i, patch in enumerate(bp["boxes"]):
                patch.set_facecolor(fill_colors[i % 2])
                patch.set_alpha(0.75)

            # Formatted x-axis labels
            ax.set_xticks(range(1, n_groups + 1))
            ax.set_xticklabels(
                [format_config_label(c) for c in configs],
                rotation=0, ha="center", fontsize=7.5
            )
            ax.grid(axis="y", linestyle="--", linewidth=0.5, color="#cccccc", alpha=0.8)
            ax.grid(axis="x", visible=False)

            ax.set_xlabel("Experimental Configuration")
            ax.set_ylabel("Detection Latency (ms)")
            ax.set_title("Distribution of Failure Detection Latency by Configuration",
                         pad=8)
            ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=6, integer=True))

            fig.tight_layout()
            fig.savefig(plots_dir / "detection_latency_boxplot.png")
            plt.close(fig)
            plots_generated += 1
            print("Saved detection_latency_boxplot.png")

    if plots_generated == 0:
        print("No data available for plotting. Run experiments first.")
    else:
        print(f"\nGenerated {plots_generated} publication-quality plots in {plots_dir}")


if __name__ == "__main__":
    main()
