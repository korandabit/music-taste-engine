#!/usr/bin/env bash
# Regenerates data/music.db from source files.
# Run before pack.sh if source data has changed.
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

python3 "$SCRIPT_DIR/consolidate.py" \
  --csv "$SCRIPT_DIR/data/edgarturtleblot.csv" \
  --spotify-dir "$SCRIPT_DIR/data" \
  --out "$SCRIPT_DIR/data/music.db"

python3 "$SCRIPT_DIR/engine.py" signals \
  --input "$SCRIPT_DIR/data/StreamingHistory"*.json \
  --db "$SCRIPT_DIR/data/music.db"
