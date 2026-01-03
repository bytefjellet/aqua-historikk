import os
import re
import sys
import gzip
import requests
from datetime import datetime

DUMP_URL = "https://api.fiskeridir.no/pub-aqua/api/v1/dump/new-legacy-csv"

def parse_snapshot_date(first_line: str) -> str:
    """
    Prøver å finne en dato i første linje (metadata-linje).
    Returnerer YYYY-MM-DD.
    Hvis vi ikke finner noe, bruker vi dagens UTC-dato.
    """
    # Finn første forekomst av noe som ligner YYYY-MM-DD
    m = re.search(r"(\d{4}-\d{2}-\d{2})", first_line)
    if m:
        return m.group(1)

    # Finn noe som ligner DD.MM.YYYY
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", first_line)
    if m:
        dd, mm, yyyy = m.group(1), m.group(2), m.group(3)
        return f"{yyyy}-{mm}-{dd}"

    # fallback
    return datetime.utcnow().strftime("%Y-%m-%d")


def main(out_dir: str):
    os.makedirs(out_dir, exist_ok=True)

    r = requests.get(DUMP_URL, timeout=120)
    r.raise_for_status()

    # CSV-innhold (tekst)
    text = r.text
    lines = text.splitlines()
    if len(lines) < 2:
        raise RuntimeError("CSV ser for kort ut (mangler minst 2 linjer).")

    first_line = lines[0]
    snapshot_date = parse_snapshot_date(first_line)

    raw_csv_path = os.path.join(out_dir, f"raw_{snapshot_date}.csv")
    cleaned_csv_path = os.path.join(out_dir, f"clean_{snapshot_date}.csv")

    # Lagre rå CSV (inkl linje 1)
    with open(raw_csv_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)

    # Lagre “clean” CSV (uten linje 1), så Python csv-leser får riktig header
    with open(cleaned_csv_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines[1:]) + "\n")

    # Gzip råfilen (for release)
    raw_gz_path = raw_csv_path + ".gz"
    with open(raw_csv_path, "rb") as fin, gzip.open(raw_gz_path, "wb") as fout:
        fout.writelines(fin)

    print(snapshot_date)
    print(raw_gz_path)
    print(cleaned_csv_path)


if __name__ == "__main__":
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "work"
    main(out_dir)
