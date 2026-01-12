#!/usr/bin/env bash
set -e

source .venv/bin/activate

python3 -m src.fetch_daily
python3 -m src.ingest
python3 -m src.build_history
python3 -m src.validate_db
python3 -m src.publish_db
