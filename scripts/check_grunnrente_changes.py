#!/usr/bin/env python3
import argparse
import json
import os
import smtplib
import ssl
import sys
from email.message import EmailMessage
from pathlib import Path
import sqlite3


def iso10(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    return s[:10] if len(s) >= 10 else s


def parse_row_json(row_json_text: str) -> dict:
    try:
        return json.loads(row_json_text) if row_json_text else {}
    except Exception:
        return {}


def extract_owner_orgnr(d: dict) -> str:
    # Basert på app.js-en din: OK_ORGNR / OK_NAVN brukes
    raw = str(d.get("OK_ORGNR", "")).strip().replace(" ", "")
    return raw if raw.isdigit() and len(raw) == 9 else ""


def extract_owner_name(d: dict) -> str:
    return str(d.get("OK_NAVN", "")).strip()


def get_latest_two_snapshot_dates(con: sqlite3.Connection) -> tuple[str | None, str | None]:
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT snapshot_date
        FROM snapshots
        WHERE snapshot_date IS NOT NULL AND TRIM(snapshot_date) <> ''
        ORDER BY date(snapshot_date) DESC
        LIMIT 2;
        """
    ).fetchall()
    d2 = iso10(rows[0][0]) if len(rows) >= 1 else None
    d1 = iso10(rows[1][0]) if len(rows) >= 2 else None
    return d1, d2


def load_grunnrente_owner_map(con: sqlite3.Connection, date_iso: str) -> dict:
    """
    Returnerer:
      orgnr -> { orgnr, name, count, permits[] }
    """
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT permit_key, row_json
        FROM permit_snapshot
        WHERE grunnrente_pliktig = 1
          AND date(snapshot_date) = date(?);
        """,
        (date_iso,),
    ).fetchall()

    m: dict[str, dict] = {}

    for permit_key, row_json_text in rows:
        d = parse_row_json(row_json_text)
        orgnr = extract_owner_orgnr(d)
        if not orgnr:
            continue

        name = extract_owner_name(d)
        pk = str(permit_key or "").strip()

        if orgnr not in m:
            m[orgnr] = {"orgnr": orgnr, "name": name or "", "count": 0, "permits": []}

        obj = m[orgnr]
        obj["count"] += 1
        if pk:
            obj["permits"].append(pk)
        if not obj["name"] and name:
            obj["name"] = name

    # dedupliser permits og sorter
    for obj in m.values():
        obj["permits"] = sorted(set(obj["permits"]))
    return m


def build_report(d1: str, d2: str, started: list[dict], stopped: list[dict]) -> str:
    lines = []
    lines.append(f"Grunnrente-endringer oppdaget ({d1} -> {d2})")
    lines.append("")
    if started:
        lines.append(f"STARTET ({len(started)}):")
        for r in started:
            nm = r.get("name") or "—"
            lines.append(f"- {nm} ({r['orgnr']}): 0 -> {r['after_count']} grunnrentepliktige tillatelser")
            if r.get("after_permits"):
                lines.append(f"  Tillatelser: {', '.join(r['after_permits'])}")
        lines.append("")
    if stopped:
        lines.append(f"SLUTTET ({len(stopped)}):")
        for r in stopped:
            nm = r.get("name") or "—"
            lines.append(f"- {nm} ({r['orgnr']}): {r['before_count']} -> 0 grunnrentepliktige tillatelser")
            if r.get("before_permits"):
                lines.append(f"  Tillatelser: {', '.join(r['before_permits'])}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def send_email(subject: str, body: str) -> None:
    # Konfig via env
    host = os.environ.get("SMTP_HOST", "").strip()
    port = int(os.environ.get("SMTP_PORT", "587").strip() or "587")
    user = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASS", "").strip()

    mail_from = os.environ.get("MAIL_FROM", "").strip()
    mail_to = os.environ.get("MAIL_TO", "").strip()

    if not (host and user and password and mail_from and mail_to):
        raise RuntimeError(
            "Mangler SMTP_* / MAIL_* env. "
            "Sett SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_FROM, MAIL_TO."
        )

    msg = EmailMessage()
    msg["From"] = mail_from
    msg["To"] = mail_to
    msg["Subject"] = subject
    msg.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP(host, port, timeout=60) as server:
        server.ehlo()
        # STARTTLS på 587
        server.starttls(context=context)
        server.ehlo()
        server.login(user, password)
        server.send_message(msg)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="db/aqua.sqlite", help="Path til aqua.sqlite")
    ap.add_argument("--state-file", default=".state/grunnrente_last_checked.txt",
                    help="Fil for å unngå duplikat-varsler (lagres med siste d2).")
    ap.add_argument("--send-email", action="store_true", help="Send e-post hvis endringer.")
    ap.add_argument(
        "--test-email",
        action="store_true",
        help="Send en test-e-post uansett (bruker SMTP_* / MAIL_* env)."
        )
    ap.add_argument(
        "--write-report",
        default=".state/grunnrente_report.txt",
        help="Skriv rapport til denne filen."
        )

    args = ap.parse_args()

    if args.test_email:
        send_email(
            subject="[Aqua-historikk] Test e-post (grunnrente-varsling)",
            body="Dette er en test for å verifisere at SMTP-oppsettet fungerer.\n"
        )
        print("Test e-post sendt.")
        return 0

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"DB finnes ikke: {db_path}", file=sys.stderr)
        return 1

    state_path = Path(args.state_file)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    report_path = Path(args.write_report)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    last_checked = state_path.read_text(encoding="utf-8").strip() if state_path.exists() else ""

    con = sqlite3.connect(str(db_path))
    try:
        d1, d2 = get_latest_two_snapshot_dates(con)
        if not d1 or not d2:
            print("Fant ikke to snapshot-datoer å sammenligne. Avslutter.")
            return 0

        # Hvis vi allerede har sjekket denne d2, ikke spam
        if last_checked == d2:
            print(f"Allerede sjekket d2={d2}. Ingen ny varsling.")
            return 0

        before = load_grunnrente_owner_map(con, d1)
        after = load_grunnrente_owner_map(con, d2)

        started = []
        stopped = []

        all_orgs = sorted(set(before.keys()) | set(after.keys()))
        for orgnr in all_orgs:
            b = before.get(orgnr)
            a = after.get(orgnr)
            bcnt = int(b["count"]) if b else 0
            acnt = int(a["count"]) if a else 0

            if bcnt == 0 and acnt > 0:
                started.append({
                    "orgnr": orgnr,
                    "name": (a or {}).get("name") or (b or {}).get("name") or "",
                    "after_count": acnt,
                    "after_permits": (a or {}).get("permits", []),
                })
            elif bcnt > 0 and acnt == 0:
                stopped.append({
                    "orgnr": orgnr,
                    "name": (a or {}).get("name") or (b or {}).get("name") or "",
                    "before_count": bcnt,
                    "before_permits": (b or {}).get("permits", []),
                })

        if not started and not stopped:
            print(f"Ingen grunnrente-endringer mellom {d1} og {d2}.")
            # markér som sjekket for å unngå gjentatte runs samme dag
            state_path.write_text(d2, encoding="utf-8")
            report_path.write_text(f"Ingen grunnrente-endringer ({d1} -> {d2}).\n", encoding="utf-8")
            return 0

        # Sortering: mest “tunge” først
        started.sort(key=lambda r: (-int(r["after_count"]), r["orgnr"]))
        stopped.sort(key=lambda r: (-int(r["before_count"]), r["orgnr"]))

        report = build_report(d1, d2, started, stopped)
        report_path.write_text(report, encoding="utf-8")

        subject = f"[Aqua-historikk] Grunnrente-endring ({d2}) – startet:{len(started)} sluttet:{len(stopped)}"
        print(report)

        if args.send_email:
            send_email(subject, report)

        # Markér som sjekket etter vellykket rapport (og evt e-post)
        state_path.write_text(d2, encoding="utf-8")
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
