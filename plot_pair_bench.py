#!/usr/bin/env python3
"""
plot_pair_bench.py — reads pair_bench.csv and generates pair_bench.png:
  Stacked bar chart: Load (ms) + Search (ms) = Total Time per thread count.
  Also annotates each bar with the distance found.
"""

import sys
import os
import matplotlib
matplotlib.use("Agg")
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

SRC  = sys.argv[1] if len(sys.argv) > 1 else "?"
DST  = sys.argv[2] if len(sys.argv) > 2 else "?"
CSV  = "pair_bench.csv"

if not os.path.exists(CSV):
    sys.exit(f"ERROR: '{CSV}' not found. Run benchmark_pair.sh first.")

df = pd.read_csv(CSV)
threads = df["threads"].tolist()
labels  = [str(t) for t in threads]
x       = range(len(threads))

load_ms   = df["load_ms"].tolist()
search_ms = df["search_ms"].tolist()
total_ms  = df["total_ms"].tolist()
distance  = df["distance"].iloc[0]   # same for all rows (fixed pair)

LOAD_COLOR   = "#4e79a7"
SEARCH_COLOR = "#f28e2b"

fig, ax = plt.subplots(figsize=(9, 6))
fig.suptitle(
    f"Node {SRC} → Node {DST}  |  Distance: {distance:.2f}",
    fontsize=13, fontweight="bold"
)

bars_load   = ax.bar(x, load_ms,   label="Load (ms)",   color=LOAD_COLOR,   alpha=0.88, edgecolor="black", linewidth=0.8)
bars_search = ax.bar(x, search_ms, bottom=load_ms, label="Search (ms)", color=SEARCH_COLOR, alpha=0.88, edgecolor="black", linewidth=0.8)

# Annotate total on top of each stacked bar.
for i, (l, s, tot) in enumerate(zip(load_ms, search_ms, total_ms)):
    ax.text(i, tot + max(total_ms) * 0.01, f"{tot:.2f}", ha="center", va="bottom",
            fontsize=9, fontweight="bold")

# Annotate load and search values inside each segment.
for i, (l, s) in enumerate(zip(load_ms, search_ms)):
    if l > max(total_ms) * 0.06:
        ax.text(i, l / 2, f"{l:.1f}", ha="center", va="center",
                fontsize=8, color="white", fontweight="bold")
    if s > max(total_ms) * 0.04:
        ax.text(i, l + s / 2, f"{s:.2f}", ha="center", va="center",
                fontsize=8, color="white", fontweight="bold")

ax.set_xticks(list(x))
ax.set_xticklabels([f"{t} thread{'s' if t > 1 else ''}" for t in threads])
ax.set_xlabel("Thread Count", fontsize=11)
ax.set_ylabel("Time (ms)", fontsize=11)
ax.set_title("Load + Search Time by Thread Count", fontsize=11)
ax.legend(fontsize=10)
ax.grid(True, axis="y", alpha=0.35, linestyle="--")
ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.1f"))

plt.tight_layout()
plt.savefig("pair_bench.png", dpi=150, bbox_inches="tight")
print("Saved pair_bench.png")
