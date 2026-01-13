#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PY="/Users/torsteinulvik/code/Aqua-historikk/.venv/bin/python"

"$PY" -m src.fetch_daily
"$PY" -m src.ingest
"$PY" -m src.build_history
"$PY" -m src.validate_db
"$PY" -m src.publish_db
