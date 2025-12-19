-- Day 4: PostgreSQL schema for UK cost of living pipeline

CREATE TABLE IF NOT EXISTS dim_series (
  series_key TEXT PRIMARY KEY,
  series_id  TEXT NOT NULL,
  series_name TEXT NOT NULL,
  unit TEXT,
  source TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
  date_id DATE PRIMARY KEY,
  year INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL,
  is_month_start BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_series_values (
  series_key TEXT NOT NULL REFERENCES dim_series(series_key),
  date_id DATE NOT NULL REFERENCES dim_date(date_id),
  value NUMERIC(18,6),
  yoy_change NUMERIC(18,6),
  rolling_3m NUMERIC(18,6),
  PRIMARY KEY (series_key, date_id)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_series_values(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_series ON fact_series_values(series_key);
