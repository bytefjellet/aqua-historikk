import pandas as pd
from src.config import CSV_SEPARATOR, CSV_ENCODING, SKIP_FIRST_LINE


def normalize_colnames(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    return df


def load_snapshot_csv(path):
    """
    Leser CSV med:
    - første linje = tittel ("AKVAKULTURTILLATELSER PR. ...") -> hoppes over
    - header på linje 2
    - semikolon-separator
    """
    df = pd.read_csv(
        path,
        sep=CSV_SEPARATOR,
        dtype=str,
        encoding=CSV_ENCODING,
        skiprows=SKIP_FIRST_LINE,
        engine="python",
    )
    return normalize_colnames(df)
