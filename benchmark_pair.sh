#!/usr/bin/env bash
set -e

GRAPH="${1:-/home/palmieri/aaa.txt}"
START="${2:-0}"
END="${3:-16828}"

if [ ! -f astar ] || [ astar.cpp -nt astar ]; then
    echo "=== Compiling astar.cpp ===" >&2
    g++ -O3 -march=native -std=c++17 -pthread -o astar astar.cpp
fi

echo "=== Benchmarking node $START → node $END ==="
./astar "$GRAPH" --bench "$START" "$END"

echo ""
echo "=== Generating plot ==="
python3 plot_pair_bench.py "$START" "$END"
