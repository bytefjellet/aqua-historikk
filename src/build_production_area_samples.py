from __future__ import annotations

import json
from src.db import connect, init_db


def main() -> None:
    conn = connect()
    init_db(conn)

    rows = conn.execute(
        "SELECT permit_key, raw_json FROM license_original_owner ORDER BY permit_key;"
    ).fetchall()

    picked = {}
    for permit_key, raw_json in rows:
        try:
            d = json.loads(raw_json)
        except Exception:
            continue

        pl = d.get("placement") or {}
        code = pl.get("prodAreaCode")
        status = pl.get("prodAreaStatus")

        if code is None or status is None:
            continue

        try:
            code_int = int(code)
        except Exception:
            continue

        if 1 <= code_int <= 13 and code_int not in picked:
            picked[code_int] = permit_key

        if len(picked) == 13:
            break

    for code_int, permit_key in picked.items():
        conn.execute(
            """
            INSERT INTO production_area_sample(prod_area_code, permit_key)
            VALUES (?, ?)
            ON CONFLICT(prod_area_code) DO UPDATE SET
                permit_key=excluded.permit_key
            """,
            (code_int, permit_key),
        )

    conn.commit()
    print(f"Ferdig. Fant sample for {len(picked)}/13 områder.")
    if len(picked) < 13:
        missing = [i for i in range(1, 14) if i not in picked]
        print("Mangler sample for:", missing)


if __name__ == "__main__":
    main()
