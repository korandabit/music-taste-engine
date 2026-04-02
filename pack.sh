#!/usr/bin/env bash
# Creates music-data-engine.zip ready for upload.
# Run build.sh first if source data has changed.
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -W)"  # Windows path for PowerShell
OUT="$SCRIPT_DIR\\music-data-engine.zip"

powershell -Command "
  \$dir = '$SCRIPT_DIR'
  \$files = @(
    'SKILL.md',
    'consolidate.py',
    'engine.py'
  ) | ForEach-Object { Join-Path \$dir \$_ }
  \$db = Join-Path \$dir 'data\music.db'
  \$all = \$files + \$db
  Compress-Archive -Path \$all -DestinationPath '$OUT' -Force
  Write-Host 'Created $OUT'
"
