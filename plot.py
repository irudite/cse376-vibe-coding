#!/usr/bin/env python3
"""
plot.py — reads results.csv produced by astar.cpp and generates:
  1. astar_latency.png   : box plot + mean bar chart of latency per thread count
  2. astar_speedup.png   : observed vs. ideal speedup curve
  3. Prints + saves astar_table.csv with the full results table
"""

import sys
import os
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — saves files without a display
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

CSV_FILE   = "results.csv"
PLOT_TITLE = sys.argv[1] if len(sys.argv) > 1 else "A* Parallel Benchmark"

# ─── Load ─────────────────────────────────────────────────────────────────────
if not os.path.exists(CSV_FILE):
    sys.exit(f"ERROR: '{CSV_FILE}' not found. Run ./astar first.")

df = pd.read_csv(CSV_FILE)
THREADS = sorted(df["threads"].unique().tolist())
COLORS  = ["#4e79a7", "#f28e2b", "#e15759"]
PALETTE = dict(zip(THREADS, COLORS))

# ─── Figure 1: Latency box plot + mean bar chart + distance ──────────────────
fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 5))
fig.suptitle(f"{PLOT_TITLE} — Latency", fontsize=14, fontweight="bold", y=1.01)

# Box plot
data   = [df[df["threads"] == t]["latency_s"].values for t in THREADS]
labels = [f"{t} threads" for t in THREADS]

bp = ax1.boxplot(
    data,
    tick_labels=labels,
    patch_artist=True,
    widths=0.45,
    medianprops=dict(color="black", linewidth=2),
    whiskerprops=dict(linewidth=1.2),
    capprops=dict(linewidth=1.2),
    flierprops=dict(marker="o", markersize=5, linestyle="none"),
)
for patch, color in zip(bp["boxes"], COLORS):
    patch.set_facecolor(color)
    patch.set_alpha(0.82)

ax1.set_xlabel("Thread Count", fontsize=11)
ax1.set_ylabel("Latency (s)", fontsize=11)
ax1.set_title("Latency Distribution per Thread Count", fontsize=11)
ax1.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.3f"))
ax1.grid(True, axis="y", alpha=0.35, linestyle="--")

# Mean latency bar chart with std-dev error bars
means = [df[df["threads"] == t]["latency_s"].mean() for t in THREADS]
stds  = [df[df["threads"] == t]["latency_s"].std(ddof=1)  for t in THREADS]
x_pos = range(len(THREADS))

bars = ax2.bar(
    x_pos, means,
    yerr=stds,
    color=COLORS,
    capsize=7,
    edgecolor="black",
    linewidth=0.9,
    alpha=0.87,
    error_kw=dict(elinewidth=1.5),
)
ax2.set_xticks(list(x_pos))
ax2.set_xticklabels(labels)
ax2.set_xlabel("Thread Count", fontsize=11)
ax2.set_ylabel("Mean Latency (s)", fontsize=11)
ax2.set_title("Mean Latency ± Std Dev", fontsize=11)
ax2.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.3f"))
ax2.grid(True, axis="y", alpha=0.35, linestyle="--")

# Annotate bar tops
margin = max(stds) * 0.15 if max(stds) > 0 else 0.001
for bar, m in zip(bars, means):
    ax2.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + margin,
        f"{m:.4f}s",
        ha="center", va="bottom", fontsize=9.5, fontweight="bold",
    )

# Mean distance per query bar chart
dist_means = [
    df[df["threads"] == t]["total_distance"].mean() /
    df[df["threads"] == t]["paths_found"].mean()
    for t in THREADS
]
dist_stds = [
    df[df["threads"] == t]["total_distance"].std(ddof=1) /
    df[df["threads"] == t]["paths_found"].mean()
    for t in THREADS
]

dbars = ax3.bar(
    x_pos, dist_means,
    yerr=dist_stds,
    color=COLORS,
    capsize=7,
    edgecolor="black",
    linewidth=0.9,
    alpha=0.87,
    error_kw=dict(elinewidth=1.5),
)
ax3.set_xticks(list(x_pos))
ax3.set_xticklabels(labels)
ax3.set_xlabel("Thread Count", fontsize=11)
ax3.set_ylabel("Mean Distance per Query", fontsize=11)
ax3.set_title("Path Distance per Query", fontsize=11)
ax3.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f"))
ax3.grid(True, axis="y", alpha=0.35, linestyle="--")

d_margin = max(dist_stds) * 0.15 if max(dist_stds) > 0 else dist_means[0] * 0.02
for bar, d in zip(dbars, dist_means):
    ax3.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + d_margin,
        f"{d:.2f}",
        ha="center", va="bottom", fontsize=9.5, fontweight="bold",
    )

plt.tight_layout()
plt.savefig("astar_latency.png", dpi=150, bbox_inches="tight")
print("Saved astar_latency.png")

# ─── Figure 2: Speedup ────────────────────────────────────────────────────────
fig2, ax = plt.subplots(figsize=(7, 5))

base_mean = means[0]  # 2-thread run as reference
speedup_observed = [base_mean / m for m in means]
speedup_ideal    = [t / THREADS[0] for t in THREADS]

ax.plot(
    [str(t) for t in THREADS], speedup_observed,
    "o-", color="#4e79a7", linewidth=2.2, markersize=9,
    label="Observed speedup",
)
ax.plot(
    [str(t) for t in THREADS], speedup_ideal,
    "--", color="#aaaaaa", linewidth=1.8,
    label=f"Ideal speedup (rel. {THREADS[0]} threads)",
)

for x, y in zip([str(t) for t in THREADS], speedup_observed):
    ax.annotate(
        f"{y:.2f}×",
        xy=(x, y), xytext=(0, 10), textcoords="offset points",
        ha="center", fontsize=9.5, fontweight="bold", color="#4e79a7",
    )

ax.set_xlabel("Thread Count", fontsize=11)
ax.set_ylabel(f"Speedup (relative to {THREADS[0]} threads)", fontsize=11)
ax.set_title(f"{PLOT_TITLE} — Speedup", fontsize=13, fontweight="bold")
ax.legend(fontsize=10)
ax.grid(True, alpha=0.35, linestyle="--")
ax.set_ylim(bottom=0)

plt.tight_layout()
plt.savefig("astar_speedup.png", dpi=150, bbox_inches="tight")
print("Saved astar_speedup.png")

# ─── Table ────────────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("  A* Benchmark Results — Total Distance per Run & Configuration")
print("=" * 70)

table = df[["threads", "run", "latency_s", "total_distance", "paths_found"]].copy()
table.columns = ["Threads", "Run", "Latency (s)", "Total Distance", "Paths Found"]
table["Latency (s)"]    = table["Latency (s)"].map("{:.4f}".format)
table["Total Distance"] = table["Total Distance"].map("{:.2f}".format)

print(table.to_string(index=False))
print("=" * 70)

# Summary stats per thread count
print()
print("  Summary (mean ± std over runs)")
print("-" * 50)
for t in THREADS:
    sub = df[df["threads"] == t]
    print(
        f"  {t:>2} threads | "
        f"latency {sub['latency_s'].mean():.4f} ± {sub['latency_s'].std(ddof=1):.4f} s | "
        f"dist {sub['total_distance'].mean():.2f}"
    )
print()

table.to_csv("astar_table.csv", index=False)
print("Table saved to astar_table.csv")
