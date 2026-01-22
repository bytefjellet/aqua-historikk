#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PY="/Users/torsteinulvik/code/Aqua-historikk/.venv/bin/python"

"$PY" -m src.fetch_daily
"$PY" -m src.ingest
"$PY" -m src.build_history
"$PY" -m src.update_transfers_daily --limit 300 --pace 0.2 --timeout 60 --max-rate-limited 20 --changed-since-days 7 --stale-after-days 30
"$PY" -m src.build_original_owner
"$PY" -m src.update_production_area_status
"$PY" -m src.validate_db

# 🔔 Grunnrente-varsling (kun ved endring)
"$PY" scripts/check_grunnrente_changes.py \
  --db db/aqua.sqlite \
  --write-report .state/grunnrente_report.txt \
  --send-email || echo "WARN: grunnrente-varsling feilet"


"$PY" -m src.publish_db

# --- Commit & push til GitHub Pages ---
cd "/Users/torsteinulvik/code/Aqua-historikk"

git add web/data/aqua.sqlite

if git diff --cached --quiet; then
  echo "No changes to commit."
else
  git commit -m "daily: update aqua.sqlite $(date +%F)"
  git push
fi
