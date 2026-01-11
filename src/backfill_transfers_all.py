# src/backfill_transfers_all.py
from __future__ import annotations

import time
from typing import List, Tuple

from src.db import connect, init_db
from src.transfers import fetch_transfers, upsert_transfers


def update_current_ownership_from_latest_transfer(conn, permit_key: str) -> bool:
    """
    Oppdaterer ownership_history for nåværende periode (valid_to IS NULL) basert på
    nyeste transfer-data som finnes i license_transfers.

    Returnerer True hvis vi oppdaterte en rad, ellers False.
    """
    cur = conn.cursor()

    # 1) Finn nåværende eier i ownership_history
    cur.execute(
        """
        SELECT owner_orgnr, valid_from
        FROM ownership_history
        WHERE permit_key = ? AND valid_to IS NULL
        ORDER BY valid_from DESC
        LIMIT 1;
        """,
        (permit_key,),
    )
    row = cur.fetchone()
    if not row:
        return False

    owner_orgnr, valid_from = row
    owner_orgnr = (owner_orgnr or "").strip()
    if not owner_orgnr:
        return False

    # 2) Finn nyeste transfer som matcher permit_key + orgnr
    cur.execute(
        """
        SELECT id, journal_date, updated_at
        FROM license_transfers
        WHERE permit_key = ?
          AND current_owner_orgnr = ?
        ORDER BY COALESCE(journal_date, updated_at) DESC
        LIMIT 1;
        """,
        (permit_key, owner_orgnr),
    )
    trow = cur.fetchone()
    if not trow:
        return False

    transfer_id, journal_date, updated_at = trow
    registered_from = journal_date or updated_at

    # 3) Oppdater nåværende ownership-periode hvis felt ikke allerede er satt
    cur.execute(
        """
        UPDATE ownership_history
        SET registered_from = COALESCE(registered_from, ?),
            transfer_id      = COALESCE(transfer_id, ?)
        WHERE permit_key = ?
          AND valid_to IS NULL
          AND owner_orgnr = ?;
        """,
        (registered_from, transfer_id, permit_key, owner_orgnr),
    )

    changed = cur.rowcount > 0
    if changed:
        conn.commit()
    return changed


def main(
    sleep_seconds: float = 0.2,
    limit: int | None = None,
) -> None:
    conn = connect()
    init_db(conn)

    # Hent alle permit_keys vi har i dag
    rows: List[Tuple[str]] = conn.execute(
        "SELECT permit_key FROM permit_current ORDER BY permit_key;"
    ).fetchall()
    keys = [r[0] for r in rows]

    if limit is not None:
        keys = keys[:limit]

    print(f"Backfill: {len(keys)} tillatelser fra permit_current")

    ok_fetch = 0
    fail_fetch = 0
    ok_update = 0
    fail_update = 0

    for i, k in enumerate(keys, start=1):
        try:
            data = fetch_transfers(k)
            upsert_transfers(conn, k, data)
            ok_fetch += 1
        except Exception as e:
            fail_fetch += 1
            print(f"[{i}/{len(keys)}] FEIL fetch {k}: {e}")
            # gå videre
            time.sleep(sleep_seconds)
            continue

        # Prøv å koble mot nåværende ownership-periode
        try:
            if update_current_ownership_from_latest_transfer(conn, k):
                ok_update += 1
        except Exception as e:
            fail_update += 1
            print(f"[{i}/{len(keys)}] FEIL update ownership {k}: {e}")

        if i % 50 == 0:
            total_transfers = conn.execute("SELECT COUNT(*) FROM license_transfers;").fetchone()[0]
            print(
                f"[{i}/{len(keys)}] status: fetch_ok={ok_fetch} fetch_fail={fail_fetch} "
                f"update_ok={ok_update} update_fail={fail_update} transfers_total={total_transfers}"
            )

        time.sleep(sleep_seconds)

    total_transfers = conn.execute("SELECT COUNT(*) FROM license_transfers;").fetchone()[0]
    print("\nFerdig.")
    print(f"fetch_ok={ok_fetch} fetch_fail={fail_fetch}")
    print(f"update_ok={ok_update} update_fail={fail_update}")
    print(f"license_transfers total={total_transfers}")


if __name__ == "__main__":
    # Du kan teste med limit=20 først ved å sette limit=20 her,
    # eller kjøre alt ved å la limit=None.
    main(sleep_seconds=0.2, limit=None)
