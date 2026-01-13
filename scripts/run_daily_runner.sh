#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="/Users/torsteinulvik/code/Aqua-historikk"
RUN_SCRIPT="$PROJECT_ROOT/run_daily.sh"

LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

STAMP="$(/bin/date +%Y-%m-%d)"
LOG_FILE="$LOG_DIR/run_daily_${STAMP}.log"
DONE_FILE="$LOG_DIR/run_daily_${STAMP}.done"

ts() { /bin/date "+%Y-%m-%dT%H:%M:%S%z"; }

notify_error() {
  /usr/bin/osascript <<'EOF2' >/dev/null 2>&1 || true
display notification "Daglig kjøring FEILET – sjekk logs/" with title "Aqua-historikk"
EOF2
}

on_error() {
  echo "!! $(ts) Runner FAILED"
  notify_error
}

trap on_error ERR

LOCK_DIR="/tmp/aqua_historikk_run_daily.lockdir"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "==> $(ts) Runner start"
cd "$PROJECT_ROOT"

if [[ -f "$DONE_FILE" ]]; then
  echo "==> $(ts) Already completed today ($STAMP). Exiting."
  exit 0
fi

# ---- Lock (macOS-safe) ----
if ! /bin/mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "!! $(ts) Another run is already running. Exiting."
  exit 0
fi

cleanup() {
  /bin/rm -rf "$LOCK_DIR"
  echo "==> $(ts) Lock released"
}
trap cleanup EXIT INT TERM

# ---- Ensure venv is active (launchd has minimal environment) ----
if [[ -f "$PROJECT_ROOT/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.venv/bin/activate"
else
  echo "!! $(ts) Missing venv at $PROJECT_ROOT/.venv"
  exit 1
fi

# ---- Run the actual job ----
"$RUN_SCRIPT"

# ---- Auto-commit & push (ONLY if DB changed) ----
if [[ -n "$(git status --porcelain web/data/aqua.sqlite)" ]]; then
  echo "==> $(ts) Database changed – committing"

  # Abort if unexpected changes exist
  if [[ -n "$(git status --porcelain | grep -v '^.. web/data/aqua\.sqlite$')" ]]; then
    echo "!! $(ts) Unexpected changes in repo – aborting commit/push"
    git status --porcelain
    exit 1
  fi

  git add web/data/aqua.sqlite
  git commit -m "Update aqua.sqlite ($( /bin/date +%Y-%m-%d ))"

  echo "==> $(ts) Pushing to origin"
  git push
else
  echo "==> $(ts) No database changes – skipping git commit"
fi

echo "==> $(ts) Runner done"
touch "$DONE_FILE"
