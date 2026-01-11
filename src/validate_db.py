from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import List, Optional

from src.db import connect


@dataclass
class Check:
    name: str
    sql: str
    expect_zero: bool = True
    warn_only: bool = False
    details_sql: Optional[str] = None


CHECKS: List[Check] = [
    Check(
        name="Duplikatperioder i ownership_history (per owner_identity)",
        sql="""
        SELECT COUNT(*) AS dup_groups
        FROM (
          SELECT 1
          FROM ownership_history
          GROUP BY permit_key, owner_identity, valid_from, COALESCE(valid_to,'NULL')
          HAVING COUNT(*) > 1
        );
        """,
        details_sql="""
        SELECT permit_key, owner_identity, owner_orgnr, owner_name, valid_from, valid_to, COUNT(*) AS n
        FROM ownership_history
        GROUP BY permit_key, owner_identity, valid_from, valid_to
        HAVING COUNT(*) > 1
        ORDER BY n DESC
        LIMIT 20;
        """,
    ),
    Check(
        name="Negative perioder (valid_to < valid_from)",
        sql="""
        SELECT COUNT(*) AS negative_periods
        FROM ownership_history
        WHERE valid_to IS NOT NULL
          AND valid_to != ''
          AND date(valid_to) < date(valid_from);
        """,
        details_sql="""
        SELECT permit_key, owner_identity, owner_orgnr, valid_from, valid_to
        FROM ownership_history
        WHERE valid_to IS NOT NULL
          AND valid_to != ''
          AND date(valid_to) < date(valid_from)
        LIMIT 20;
        """,
    ),
    Check(
        name="Flere aktive perioder per permit",
        sql="""
        SELECT COUNT(*) AS permits_with_multiple_active
        FROM (
          SELECT permit_key
          FROM ownership_history
          WHERE valid_to IS NULL OR valid_to = ''
          GROUP BY permit_key
          HAVING COUNT(*) > 1
        );
        """,
        details_sql="""
        SELECT permit_key, COUNT(*) AS active_periods
        FROM ownership_history
        WHERE valid_to IS NULL OR valid_to = ''
        GROUP BY permit_key
        HAVING COUNT(*) > 1
        ORDER BY active_periods DESC
        LIMIT 50;
        """,
    ),
    Check(
        name="Overlapp i ownership_history per permit",
        sql="""
        WITH s AS (
          SELECT
            permit_key,
            owner_identity,
            date(valid_from) AS vf,
            date(COALESCE(NULLIF(valid_to,''), '9999-12-31')) AS vt,
            LAG(date(COALESCE(NULLIF(valid_to,''), '9999-12-31')))
              OVER (PARTITION BY permit_key ORDER BY date(valid_from), id) AS prev_vt
          FROM ownership_history
        )
        SELECT COUNT(*) AS overlaps
        FROM s
        WHERE prev_vt IS NOT NULL
          AND vf <= prev_vt;
        """,
        details_sql="""
        WITH s AS (
          SELECT
            id,
            permit_key,
            owner_identity,
            date(valid_from) AS vf,
            date(COALESCE(NULLIF(valid_to,''), '9999-12-31')) AS vt,
            LAG(date(COALESCE(NULLIF(valid_to,''), '9999-12-31')))
              OVER (PARTITION BY permit_key ORDER BY date(valid_from), id) AS prev_vt
          FROM ownership_history
        )
        SELECT permit_key, owner_identity, vf, vt, prev_vt
        FROM s
        WHERE prev_vt IS NOT NULL
          AND vf <= prev_vt
        LIMIT 50;
        """,
    ),
    Check(
        name="permit_current matcher aktiv ownership_history (owner_identity)",
        sql="""
        SELECT COUNT(*) AS current_without_matching_active_owner
        FROM permit_current pc
        LEFT JOIN ownership_history oh
          ON oh.permit_key = pc.permit_key
         AND (oh.valid_to IS NULL OR oh.valid_to = '')
        WHERE oh.permit_key IS NULL
           OR COALESCE(oh.owner_identity,'') != COALESCE(pc.owner_identity,'');
        """,
        details_sql="""
        SELECT pc.permit_key,
               pc.owner_identity AS pc_owner_identity,
               oh.owner_identity AS oh_owner_identity,
               pc.owner_orgnr AS pc_orgnr,
               oh.owner_orgnr AS oh_orgnr
        FROM permit_current pc
        LEFT JOIN ownership_history oh
          ON oh.permit_key = pc.permit_key
         AND (oh.valid_to IS NULL OR oh.valid_to = '')
        WHERE oh.permit_key IS NULL
           OR COALESCE(oh.owner_identity,'') != COALESCE(pc.owner_identity,'')
        LIMIT 50;
        """,
    ),
    Check(
        name="permit_current.snapshot_date etter siste snapshots.snapshot_date",
        sql="""
        WITH maxsnap AS (SELECT MAX(snapshot_date) AS max_date FROM snapshots)
        SELECT COUNT(*) AS current_after_last_snapshot
        FROM permit_current, maxsnap
        WHERE maxsnap.max_date IS NOT NULL
          AND date(permit_current.snapshot_date) > date(maxsnap.max_date);
        """,
        details_sql="""
        WITH maxsnap AS (SELECT MAX(snapshot_date) AS max_date FROM snapshots)
        SELECT permit_key, snapshot_date, (SELECT max_date FROM maxsnap) AS max_date
        FROM permit_current
        WHERE date(snapshot_date) > date((SELECT max_date FROM maxsnap))
        LIMIT 50;
        """,
    ),
]


def fetch_one(conn: sqlite3.Connection, sql: str) -> int:
    cur = conn.execute(sql)
    row = cur.fetchone()
    if row is None:
        return 0
    v = row[0]
    try:
        return int(v)
    except Exception:
        return 0


def fetch_rows(conn: sqlite3.Connection, sql: str) -> List[tuple]:
    cur = conn.execute(sql)
    rows = cur.fetchall()
    out: List[tuple] = []
    for r in rows:
        try:
            out.append(tuple(r))  # sqlite3.Row -> tuple
        except Exception:
            out.append(r)
    return out


def main() -> int:
    conn = connect()
    db_list = conn.execute("PRAGMA database_list;").fetchall()
    print("Using database:", db_list)

    failed: List[str] = []
    warned: List[str] = []

    print("\nDB Validation Report\n" + "-" * 60)

    for chk in CHECKS:
        count = fetch_one(conn, chk.sql)
        ok = (count == 0) if chk.expect_zero else (count > 0)

        status = "OK" if ok else ("WARN" if chk.warn_only else "FAIL")
        print(f"{status:4}  {chk.name}: {count}")

        if not ok:
            if chk.warn_only:
                warned.append(chk.name)
            else:
                failed.append(chk.name)

            if chk.details_sql:
                rows = fetch_rows(conn, chk.details_sql)
                if rows:
                    print("      Eksempler:")
                    for r in rows[:10]:
                        print("       -", r)

    print("-" * 60)
    if failed:
        print(f"FAILED checks ({len(failed)}):")
        for n in failed:
            print(" -", n)
        return 2

    if warned:
        print(f"WARNINGS ({len(warned)}):")
        for n in warned:
            print(" -", n)

    print("All critical checks passed ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
