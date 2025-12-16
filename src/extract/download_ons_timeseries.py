import logging
from datetime import datetime
from pathlib import Path
import requests

ONS_SERIES = {
    "cpih_l55o": {
        "name": "CPIH annual rate (All items)",
        "series_id": "L55O",
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=%2Feconomy%2Finflationandpriceindices%2Ftimeseries%2Fl55o%2Fmm23",
    },
    "awe_kac3": {
        "name": "AWE YoY 3-month avg growth (Total pay ex arrears)",
        "series_id": "KAC3",
        "url": "https://www.ons.gov.uk/generator?format=csv&uri=%2Femploymentandlabourmarket%2Fpeopleinwork%2Fearningsandworkinghours%2Ftimeseries%2Fkac3%2Flms",
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

def download_csv(url: str, out_file: Path, logger: logging.Logger) -> None:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; uk-cost-of-living-pipeline/1.0)"}
    logger.info(f"Downloading: {url}")
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_bytes(r.content)
    logger.info(f"Saved: {out_file} ({out_file.stat().st_size} bytes)")

def run_extract(raw_dir: Path, log_path: Path) -> None:
    logger = setup_logger(log_path)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info("=== Extract started ===")

    for key, meta in ONS_SERIES.items():
        out_file = raw_dir / f"{key}_{ts}.csv"
        try:
            download_csv(meta["url"], out_file, logger)
        except Exception as e:
            logger.exception(f"FAILED: {key} ({meta['series_id']}): {e}")

    logger.info("=== Extract finished ===")

if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[2]
    raw_dir = repo_root / "data" / "raw"
    log_path = repo_root / "logs" / "pipeline.log"
    run_extract(raw_dir, log_path)
