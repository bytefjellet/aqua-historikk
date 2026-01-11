from __future__ import annotations
from pathlib import Path
import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
FILTER_PATH = BASE_DIR / "config" / "filter.yml"

def load_rules(filter_name: str = "Grunnrenteskatteplikt"):
    data = yaml.safe_load(FILTER_PATH.read_text(encoding="utf-8"))
    f = next(x for x in data["filters"] if x["name"] == filter_name)
    return f["rules"]

RULES = load_rules()

def is_grunnrente_pliktig(row: dict) -> bool:
    """
    row: dict med kolonnenavn->verdi (fra CSV)
    Alle regler må matche (AND).
    include_any: minst én token må finnes i feltet.
    """
    for r in RULES:
        col = r["col"]
        include_any = r["include_any"]
        val = str(row.get(col, "") or "").strip()

        if not any(token in val for token in include_any):
            return False
    return True
