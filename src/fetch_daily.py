from datetime import date
from pathlib import Path
import subprocess

from src.config import SNAPSHOT_DIR

URL = "https://api.fiskeridir.no/pub-aqua/api/v1/dump/new-legacy-csv"

def main():
    today = date.today().isoformat()
    out = SNAPSHOT_DIR / f"{today} - Uttrekk fra Akvakulturregisteret.csv"

    if out.exists():
        print(f"Finnes allerede: {out.name}")
        return

    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    subprocess.run([
        "curl",
        "-s",
        "-X", "GET", URL,
        "-H", "accept: text/plain;charset=UTF-8",
        "-o", str(out)
    ], check=True)

    print(f"Lastet ned {out.name}")

if __name__ == "__main__":
    main()
