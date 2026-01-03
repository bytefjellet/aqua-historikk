import csv
import json
import os
import sys
import hashlib
import sqlite3
from datetime import datetime, timedelta
import yaml

PERMIT_COL = "TILL_NR"
HOLDER_COL = "ORG.NR/PERS.NR"


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def detect_delimiter(sample_text: str) -> str:
    # Dumpen din er semikolon-separert, men vi gjør dette robust.
    if sample_text.count(";") >= sample_text.count(","):
        return ";"
    return ","


def load_filters(path: str):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    return doc.get("filters", []) or []


def rules_match(row: dict, rules: list) -> bool:
    """
    rules:
      - col: "KOLONNENAVN"
        include_any: ["A", "B"]  (case-sensitive)
      - col: "KOLONNENAVN"
        exclude_any: ["X", "Y"]
    Alle regler må bestås (AND).
    Hvis col ikke finnes: regelen ignoreres (for robusthet).
    """
    for rule in rules:
        col = rule.get("col")
        if not col or col not in row:
            continue
        val = (row.get(col) or "").strip()
        include_any = rule.get("include_any")
        exclude_any = rule.get("exclude_any")

        if include_any is not None and val not in include_any:
            return False
        if exclude_any is not None and val in exclude_any:
            return False
    return True


def ensure_schema(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS permit_snapshot (
        snapshot_date TEXT NOT NULL,
        permit_id TEXT NOT NULL,
        holder_id TEXT NOT NULL,
        row_hash TEXT NOT NULL,
        row_json TEXT NOT NULL,
        PRIMARY KEY (snapshot_date, permit_id, holder_id, row_hash)
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS ownership_intervals (
        permit_id TEXT NOT NULL,
        holder_id TEXT NOT NULL,
        valid_from TEXT NOT NULL,
        valid_to TEXT,
        source TEXT NOT NULL DEFAULT 'daily_dump',
        PRIMARY KEY (permit_id, holder_id, valid_from)
    )
    """)

    # Tagger/filtre per snapshot
    conn.execute("""
    CREATE TABLE IF NOT EXISTS permit_tags (
        snapshot_date TEXT NOT NULL,
        permit_id TEXT NOT NULL,
        tag TEXT NOT NULL,
        PRIMARY KEY (snapshot_date, permit_id, tag)
    )
    """)

    # Indekser
    conn.execute("CREATE INDEX IF NOT EXISTS idx_own_holder_current ON ownership_intervals(holder_id, valid_to)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_own_permit_current ON ownership_intervals(permit_id, valid_to)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_holder ON permit_snapshot(snapshot_date, holder_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snap_permit ON permit_snapshot(snapshot_date, permit_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tags_date_tag ON permit_tags(snapshot_date, tag)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tags_permit ON permit_tags(permit_id)")

    conn.commit()


def get_last_snapshot_date(conn: sqlite3.Connection):
    cur = conn.execute("SELECT value FROM meta WHERE key='last_snapshot_date'")
    row = cur.fetchone()
    return row[0] if row else None


def set_last_snapshot_date(conn: sqlite3.Connection, snapshot_date: str):
    conn.execute(
        "INSERT INTO meta(key, value) VALUES('last_snapshot_date', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (snapshot_date,),
    )
    conn.commit()


def parse_date(d: str) -> datetime:
    return datetime.strptime(d, "%Y-%m-%d")


def fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def build_today_data(clean_csv_path: str, snapshot_date: str, filters: list):
    """
    Returnerer:
      today_owner_by_permit: dict permit_id -> holder_id (siste sett)
      snapshots: list av (snapshot_date, permit_id, holder_id, row_hash, row_json)
      tag_rows: list av (snapshot_date, permit_id, tag)
      cols: kolonner (for debugging)
    """
    today_owner_by_permit = {}
    snapshots = []
    tag_rows = []

    with open(clean_csv_path, "r", encoding="utf-8") as f:
        sample = f.read(8192)
        f.seek(0)
        delim = detect_delimiter(sample)

        reader = csv.DictReader(f, delimiter=delim)
        cols = reader.fieldnames or []

        if PERMIT_COL not in cols or HOLDER_COL not in cols:
            raise RuntimeError(
                f"Mangler kolonner. Fant: {cols}. Må ha '{PERMIT_COL}' og '{HOLDER_COL}'."
            )

        for row in reader:
            permit_id = (row.get(PERMIT_COL) or "").strip()
            holder_id = (row.get(HOLDER_COL) or "").strip()
            if not permit_id or not holder_id:
                continue

            row_json = json.dumps(row, ensure_ascii=False, sort_keys=True)
            row_hash = sha256_text(row_json)

            snapshots.append((snapshot_date, permit_id, holder_id, row_hash, row_json))
            today_owner_by_permit[permit_id] = holder_id

            # Tagging: sjekk alle navngitte filtre
            for flt in filters:
                name = (flt.get("name") or "").strip()
                rules = flt.get("rules", []) or []
                if name and rules_match(row, rules):
                    tag_rows.append((snapshot_date, permit_id, name))

    return today_owner_by_permit, snapshots, tag_rows, cols


def apply_snapshots(conn: sqlite3.Connection, snapshots: list):
    conn.executemany("""
        INSERT OR IGNORE INTO permit_snapshot(snapshot_date, permit_id, holder_id, row_hash, row_json)
        VALUES (?, ?, ?, ?, ?)
    """, snapshots)
    conn.commit()


def apply_tags(conn: sqlite3.Connection, tag_rows: list):
    conn.executemany("""
        INSERT OR IGNORE INTO permit_tags(snapshot_date, permit_id, tag)
        VALUES (?, ?, ?)
    """, tag_rows)
    conn.commit()


def current_owner(conn: sqlite3.Connection, permit_id: str):
    cur = conn.execute("""
        SELECT holder_id
        FROM ownership_intervals
        WHERE permit_id=? AND valid_to IS NULL
        ORDER BY valid_from DESC
        LIMIT 1
    """, (permit_id,))
    row = cur.fetchone()
    return row[0] if row else None


def close_current_interval(conn: sqlite3.Connection, permit_id: str, snapshot_date: str):
    day_before = fmt_date(parse_date(snapshot_date) - timedelta(days=1))
    conn.execute("""
        UPDATE ownership_intervals
        SET valid_to=?
        WHERE permit_id=? AND valid_to IS NULL
    """, (day_before, permit_id))


def open_new_interval(conn: sqlite3.Connection, permit_id: str, holder_id: str, snapshot_date: str):
    conn.execute("""
        INSERT OR IGNORE INTO ownership_intervals(permit_id, holder_id, valid_from, valid_to, source)
        VALUES (?, ?, ?, NULL, 'daily_dump')
    """, (permit_id, holder_id, snapshot_date))


def update_intervals(conn: sqlite3.Connection, today_owner_by_permit: dict, snapshot_date: str):
    for permit_id, holder_id in today_owner_by_permit.items():
        cur_owner = current_owner(conn, permit_id)
        if cur_owner is None:
            open_new_interval(conn, permit_id, holder_id, snapshot_date)
        elif cur_owner != holder_id:
            close_current_interval(conn, permit_id, snapshot_date)
            open_new_interval(conn, permit_id, holder_id, snapshot_date)
    conn.commit()


def main(clean_csv_path: str, snapshot_date: str, sqlite_path: str, filter_path: str):
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    filters = load_filters(filter_path)

    conn = sqlite3.connect(sqlite_path)
    try:
        ensure_schema(conn)

        last = get_last_snapshot_date(conn)
        if last == snapshot_date:
            print(f"Ingenting å gjøre: snapshot_date {snapshot_date} finnes allerede.")
            return

        today_owner_by_permit, snapshots, tag_rows, cols = build_today_data(clean_csv_path, snapshot_date, filters)

        apply_snapshots(conn, snapshots)
        apply_tags(conn, tag_rows)
        update_intervals(conn, today_owner_by_permit, snapshot_date)
        set_last_snapshot_date(conn, snapshot_date)

        # Litt nyttig logging i Actions:
        print(f"OK: {snapshot_date}")
        print(f"Snapshots lagret: {len(snapshots)}")
        print(f"Unike tillatelser (for intervaller): {len(today_owner_by_permit)}")
        print(f"Tag-rader lagret: {len(tag_rows)}")
        print(f"Kolonner sett: {cols}")

    finally:
        conn.close()


if __name__ == "__main__":
    # Args:
    #   1: path til clean CSV
    #   2: snapshot_date (YYYY-MM-DD)
    #   3: sqlite path
    #   4: filter.yml
    clean_csv_path = sys.argv[1]
    snapshot_date = sys.argv[2]
    sqlite_path = sys.argv[3] if len(sys.argv) > 3 else "db/aqua.sqlite"
    filter_path = sys.argv[4] if len(sys.argv) > 4 else "config/filter.yml"
    main(clean_csv_path, snapshot_date, sqlite_path, filter_path)
