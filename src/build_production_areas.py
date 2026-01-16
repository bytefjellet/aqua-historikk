from __future__ import annotations

import json
import hashlib
from datetime import date
from typing import Dict, Optional, Tuple

from src.db import connect, init_db


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def main() -> None:
    conn = connect()
    init_db(conn)

    snapshot_date = date.today().isoformat()

    # code -> (name, status, mini_json)
    area: Dict[int, Tuple[Optional[str], Optional[str], str]] = {}

    rows = conn.execute("SELECT raw_json FROM license_original_owner;").fetchall()
    print(f"Leser {len(rows)} license_original_owner raw_json...")

    for (raw_json,) in rows:
        try:
            d = json.loads(raw_json)
        except Exception:
            continue

        placement = d.get("placement") or {}
        code = placement.get("prodAreaCode")
        name = placement.get("prodAreaName")
        status = placement.get("prodAreaStatus")

        if code is None:
            continue
        try:
            code_int = int(code)
        except Exception:
            continue
        if not (1 <= code_int <= 13):
            continue

        mini = json.dumps(
            {"prodAreaCode": code_int, "prodAreaName": name, "prodAreaStatus": status},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

        if code_int not in area:
            area[code_int] = (name, status, mini)
        else:
            prev_name, prev_status, _ = area[code_int]
            if (not prev_name) and name:
                prev_name = name
            if (not prev_status) and status:
                prev_status = status
            mini2 = json.dumps(
                {"prodAreaCode": code_int, "prodAreaName": prev_name, "prodAreaStatus": prev_status},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            area[code_int] = (prev_name, prev_status, mini2)

    print(f"Fant {len(area)} produksjonsområder (av 13) i dataene.")

    for code_int, (name, status, mini) in area.items():
        conn.execute(
            """
            INSERT INTO production_area(prod_area_code, prod_area_name)
            VALUES (?, ?)
            ON CONFLICT(prod_area_code) DO UPDATE SET
                prod_area_name=excluded.prod_area_name
            """,
            (code_int, name),
        )

        row_hash = sha256_text(mini)
        conn.execute(
            """
            INSERT OR REPLACE INTO production_area_status(
                snapshot_date, prod_area_code, prod_area_status, raw_json, row_hash
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (snapshot_date, code_int, status, mini, row_hash),
        )

    conn.commit()
    print("Ferdig ✅")


if __name__ == "__main__":
    main()
