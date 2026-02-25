#!/usr/bin/env bash
set -e

GRAPH_FILE="${1:-graph.txt}"

echo "=== Compiling astar.cpp ==="
g++ -O3 -march=native -std=c++17 -pthread -o astar astar.cpp
echo "Compiled successfully."

echo ""
echo "=== Running A* benchmark on '$GRAPH_FILE' ==="
./astar "$GRAPH_FILE"

echo ""
echo "=== Generating plots ==="
python3 plot.py
