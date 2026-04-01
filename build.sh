#!/usr/bin/env bash
# Regenerates data/music.db from source files.
# Run before pack.sh if source data has changed.
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

python3 "$SCRIPT_DIR/consolidate.py" \
  --data-dir "$SCRIPT_DIR/data" \
  --out "$SCRIPT_DIR/data/music.db"

python3 "$SCRIPT_DIR/spotify_signal_engine.py" \
  --input "$SCRIPT_DIR/data/StreamingHistory"*.json \
  --db "$SCRIPT_DIR/data/music.db"
