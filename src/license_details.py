from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import requests


BASE = "https://api.fiskeridir.no/pub-aqua/api/v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fetch_license_details(permit_key: str) -> Dict[str, Any]:
    url = f"{BASE}/licenses/{permit_key}"
    r = requests.get(url, headers={"accept": "application/json; charset=UTF-8"}, timeout=30)
    r.raise_for_status()
    return r.json()


def extract_original_owner(details: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    gi = details.get("grantInformation") or {}

    org = gi.get("openLegalEntityNr") or details.get("openLegalEntityNr")
    name = gi.get("legalEntityName") or details.get("legalEntityName")

    org = str(org).strip() if org else None
    name = str(name).strip() if name else None
    return org, name


def extract_prod_area(details: Dict[str, Any]) -> tuple[Optional[int], Optional[str], Optional[str]]:
    pl = details.get("placement") or {}
    code = pl.get("prodAreaCode")
    name = pl.get("prodAreaName")
    status = pl.get("prodAreaStatus")

    code_int: Optional[int] = None
    if code is not None:
        try:
            code_int = int(code)
        except Exception:
            code_int = None

    name = str(name).strip() if name else None
    status = str(status).strip() if status else None
    return code_int, name, status


def upsert_original_owner(conn, permit_key: str, details: Dict[str, Any]) -> None:
    org, name = extract_original_owner(details)
    raw_json = json.dumps(details, ensure_ascii=False, sort_keys=True)
    conn.execute(
        """
        INSERT INTO license_original_owner(
            permit_key, original_owner_orgnr, original_owner_name, raw_json, fetched_at
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(permit_key) DO UPDATE SET
            original_owner_orgnr=excluded.original_owner_orgnr,
            original_owner_name=excluded.original_owner_name,
            raw_json=excluded.raw_json,
            fetched_at=excluded.fetched_at
        """,
        (permit_key, org, name, raw_json, utc_now_iso()),
    )
