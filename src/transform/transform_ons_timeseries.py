import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd


SERIES_META = {
    "cpih_l55o": {
        "series_id": "L55O",
        "series_name": "CPIH annual rate (All items)",
        "unit": "percent",
        "source": "ONS",
        "prefix": "cpih_l55o_",
    },
    "awe_kac3": {
        "series_id": "KAC3",
        "series_name": "AWE YoY 3-month avg growth (Total pay ex arrears)",
        "unit": "percent",
        "source": "ONS",
        "prefix": "awe_kac3_",
    },
}


def setup_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def latest_file(raw_dir: Path, prefix: str) -> Path:
    files = sorted(raw_dir.glob(f"{prefix}*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No raw files found in {raw_dir} for prefix '{prefix}'")
    return files[0]


def read_ons_generator_csv(path: Path) -> pd.DataFrame:
    """
    ONS generator CSV for a time series often looks like:
    Title,<SERIES NAME>
    CDID,L55O
    Source,Office for National Statistics
    ... (more metadata)
    YYYY MMM,<value>
    YYYY MMM,<value>

    So we:
    - read the CSV with no header
    - find the first row that looks like a period (e.g., '2010 JAN')
    - take that row onward as the data table
    """
    df = pd.read_csv(path, header=None)

    # Find the first row where col0 looks like a date/period (e.g. "2010 JAN", "2010 FEB")
    # This is usually where the actual data starts.
    start_idx = None
    for i in range(min(len(df), 200)):
        v = str(df.iloc[i, 0]).strip()
        # Pattern like "2010 JAN" or "2010 FEB"
        if len(v) >= 8 and v[:4].isdigit():
            start_idx = i
            break

    if start_idx is None:
        raise ValueError("Could not locate start of time series rows in ONS CSV.")

    df_data = df.iloc[start_idx:].copy()
    df_data.columns = ["period", "value"]
    return df_data



def standardise_series(df_raw: pd.DataFrame, meta: dict) -> pd.DataFrame:
    # Expect columns: period, value
    if "period" not in df_raw.columns or "value" not in df_raw.columns:
        raise ValueError(f"Missing required columns in raw CSV. Found columns: {list(df_raw.columns)}")

    df = df_raw.copy()

    # Convert period like "2010 JAN" to a real date (use first day of month)
    df["date"] = pd.to_datetime(df["period"].astype(str).str.strip(), format="%Y %b", errors="coerce")
    df = df.dropna(subset=["date"])

    # Convert values
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    # Add metadata columns
    df["series_id"] = meta["series_id"]
    df["series_key"] = meta["series_key"]
    df["series_name"] = meta["series_name"]
    df["unit"] = meta["unit"]
    df["source"] = meta["source"]

    df = df.sort_values("date")

    # Derived features
    df["yoy_change"] = df["value"].pct_change(12) * 100
    df["rolling_3m"] = df["value"].rolling(3).mean()

    # Keep tidy columns
    df = df[["date", "series_id", "series_key", "series_name", "value", "unit", "source", "yoy_change", "rolling_3m"]]
    return df


def run_transform(raw_dir: Path, processed_dir: Path, log_path: Path) -> Path:
    logger = setup_logger(log_path)
    logger.info("=== Transform started ===")

    processed_dir.mkdir(parents=True, exist_ok=True)
    outputs = []

    for key, meta in SERIES_META.items():
        meta = dict(meta)  # copy
        meta["series_key"] = key

        raw_path = latest_file(raw_dir, meta["prefix"])
        logger.info(f"Using raw file for {key}: {raw_path.name}")

        df_raw = read_ons_generator_csv(raw_path)
        df_clean = standardise_series(df_raw, meta)

        outputs.append(df_clean)

    df_all = pd.concat(outputs, ignore_index=True)
    df_all = df_all.sort_values(["series_key", "date"]).reset_index(drop=True)

    out_file = processed_dir / "ons_series_long.csv"
    df_all.to_csv(out_file, index=False)

    logger.info(f"Saved processed dataset: {out_file} (rows={len(df_all)})")
    logger.info("=== Transform finished ===")
    return out_file


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[2]
    raw_dir = repo_root / "data" / "raw"
    processed_dir = repo_root / "data" / "processed"
    log_path = repo_root / "logs" / "pipeline.log"

    run_transform(raw_dir, processed_dir, log_path)
