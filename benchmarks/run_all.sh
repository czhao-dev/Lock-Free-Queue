#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

mkdir -p "$RESULTS_DIR"

echo "=== Throughput ==="
cargo run --quiet --release --manifest-path "$PROJECT_DIR/Cargo.toml" --example throughput_bench \
    | tee "$RESULTS_DIR/throughput.csv"

echo ""
echo "=== Padding ==="
cargo run --quiet --release --manifest-path "$PROJECT_DIR/Cargo.toml" --example padding_bench \
    | tee "$RESULTS_DIR/padding.csv"

echo ""
echo "=== Latency ==="
cargo run --quiet --release --manifest-path "$PROJECT_DIR/Cargo.toml" --example latency_bench \
    | tee "$RESULTS_DIR/latency.csv"

echo ""
echo "Results saved to $RESULTS_DIR/"
