#!/usr/bin/env bash
set -e

GRAPH="${1:-/home/palmieri/aaa.txt}"
QUERIES="${2:-50}"
THREADS="${3:-1}"
START_ID="${4:-}"
END_ID="${5:-}"

# Compile if binary is missing or source is newer.
if [ ! -f astar ] || [ astar.cpp -nt astar ]; then
    echo "=== Compiling astar.cpp ===" >&2
    g++ -O3 -march=native -std=c++17 -pthread -o astar astar.cpp
fi

if [ -n "$START_ID" ] && [ -n "$END_ID" ]; then
    ./astar "$GRAPH" "$QUERIES" "$THREADS" "$START_ID" "$END_ID"
else
    ./astar "$GRAPH" "$QUERIES" "$THREADS"
fi
