from __future__ import annotations

import sqlite3
from typing import List

from src.db import connect, init_db
from src.license_details import fetch_license_details, upsert_original_owner


def get_all_permits(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute("""
        SELECT pc.permit_key
        FROM permit_current pc
        LEFT JOIN license_original_owner loo ON loo.permit_key = pc.permit_key
        WHERE loo.permit_key IS NULL
        ORDER BY pc.permit_key;
    """)
    return [r[0] for r in cur.fetchall()]

    permits = get_all_permits(conn)
    print(f"Fant {len(permits)} tillatelser å hente (mangler i license_original_owner)")

    


def main() -> None:
    conn = connect()
    init_db(conn)

    permits = get_all_permits(conn)
    if not permits:
        print("Ingenting å gjøre ✅")
        return

    print(f"Fant {len(permits)} tillatelser å hente (mangler i license_original_owner)")
    
    ok = 0
    fail = 0

    import time

    for i, pk in enumerate(permits, start=1):
        try:
            details = fetch_license_details(pk)
            upsert_original_owner(conn, pk, details)
            ok += 1

            # Pause for å unngå 429
            time.sleep(0.5)

        except Exception as e:
            fail += 1
            print(f"FEIL {pk}: {e}")

        if i % 100 == 0:
            conn.commit()
            print(f"{i}/{len(permits)} ferdig...")

    conn.commit()
    print(f"Ferdig. OK={ok}, FEIL={fail}")



if __name__ == "__main__":
    main()
