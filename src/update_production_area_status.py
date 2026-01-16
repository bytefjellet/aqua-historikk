from __future__ import annotations

import json
import hashlib
from datetime import date

from src.db import connect, init_db
from src.license_details import fetch_license_details


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def latest_hash(conn, code_int: int) -> str | None:
    row = conn.execute(
        """
        SELECT row_hash
        FROM production_area_status
        WHERE prod_area_code = ?
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
        (code_int,),
    ).fetchone()
    return row[0] if row else None


def main() -> None:
    conn = connect()
    init_db(conn)

    snapshot_date = date.today().isoformat()

    samples = conn.execute(
        "SELECT prod_area_code, permit_key FROM production_area_sample ORDER BY prod_area_code;"
    ).fetchall()

    if not samples:
        raise SystemExit("Fant ingen samples. Kjør først: python -m src.build_production_area_samples")

    written = 0
    skipped = 0

    for code_int, permit_key in samples:
        details = fetch_license_details(permit_key)

        pl = details.get("placement") or {}
        status = pl.get("prodAreaStatus")
        name = pl.get("prodAreaName")
        code = pl.get("prodAreaCode")

        if code is not None:
            try:
                api_code = int(code)
            except Exception:
                api_code = None
            if api_code is not None and api_code != int(code_int):
                print(
                    f"ADVARSEL: production_area_sample mismatch: "
                    f"permit={permit_key}, expected={code_int}, got={api_code}"
)

        mini = json.dumps(
            {"prodAreaCode": code, "prodAreaName": name, "prodAreaStatus": status},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        h = sha256_text(mini)
        prev = latest_hash(conn, int(code_int))

        if prev == h:
            skipped += 1
            continue

        conn.execute(
            """
            INSERT OR REPLACE INTO production_area_status(
                snapshot_date, prod_area_code, prod_area_status, raw_json, row_hash
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (snapshot_date, int(code_int), status, mini, h),
        )
        written += 1

    conn.commit()
    print(f"Production area status: written={written}, skipped={skipped}")


if __name__ == "__main__":
    main()
