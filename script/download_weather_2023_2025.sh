#!/usr/bin/env bash
set -euo pipefail

python -m pip -q install meteostat pyarrow
python scripts/download_weather_2023_2025.py

ls -lh data_raw/weather || true
