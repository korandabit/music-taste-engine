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
    'lastfm_taste_engine.py',
    'lastfm_trajectory_engine.py',
    'spotify_signal_engine.py'
  ) | ForEach-Object { Join-Path \$dir \$_ }
  \$dataFiles = Get-ChildItem -Path (Join-Path \$dir 'data') -File |
    Where-Object { \$_.Extension -ne '.zip' } |
    Select-Object -ExpandProperty FullName
  \$all = \$files + \$dataFiles
  Compress-Archive -Path \$all -DestinationPath '$OUT' -Force
  Write-Host 'Created $OUT'
"
