import os
import logging
from pathlib import Path

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text


def setup_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.FileHandler(log_path, encoding="utf-8")
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def build_engine():
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    database = os.getenv("PGDATABASE", "cost_of_living")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url, future=True)


def load_processed_csv(csv_path: Path, engine, logger: logging.Logger):
    df = pd.read_csv(csv_path)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df = df.dropna(subset=["date", "series_key"])

    # ---------- dim_series ----------
    dim_series = (
        df[["series_key", "series_id", "series_name", "unit", "source"]]
        .drop_duplicates()
        .sort_values("series_key")
    )

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_series CASCADE;"))

    dim_series.to_sql("dim_series", engine, if_exists="append", index=False)
    logger.info(f"Loaded dim_series rows={len(dim_series)}")

    # ---------- dim_date ----------
    dim_date = pd.DataFrame({"date_id": sorted(df["date"].unique())})
    dim_date["year"] = pd.to_datetime(dim_date["date_id"]).dt.year
    dim_date["month"] = pd.to_datetime(dim_date["date_id"]).dt.month
    dim_date["day"] = pd.to_datetime(dim_date["date_id"]).dt.day
    dim_date["is_month_start"] = dim_date["day"].eq(1)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_date CASCADE;"))

    dim_date.to_sql("dim_date", engine, if_exists="append", index=False)
    logger.info(f"Loaded dim_date rows={len(dim_date)}")

    # ---------- fact_series_values ----------
    fact = df[["series_key", "date", "value", "yoy_change", "rolling_3m"]].copy()
    fact = fact.rename(columns={"date": "date_id"})
    fact = fact.drop_duplicates(subset=["series_key", "date_id"])

    for col in ["value", "yoy_change", "rolling_3m"]:
        fact[col] = pd.to_numeric(fact[col], errors="coerce")

    fact.replace([np.inf, -np.inf], np.nan, inplace=True)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fact_series_values;"))

    fact.to_sql("fact_series_values", engine, if_exists="append", index=False)
    logger.info(f"Loaded fact_series_values rows={len(fact)}")


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[2]
    log_path = repo_root / "logs" / "pipeline.log"
    logger = setup_logger(log_path)

    csv_path = repo_root / "data" / "processed" / "ons_series_long.csv"
    if not csv_path.exists():
        raise FileNotFoundError("Processed CSV not found. Run Day 3 first.")

    logger.info("=== Load started ===")
    engine = build_engine()
    load_processed_csv(csv_path, engine, logger)
    logger.info("=== Load finished ===")
