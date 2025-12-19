-- Day 5: Analysis views for Power BI
-- Assumes schema: dim_series, dim_date, fact_series_values

-- 1) Base view: monthly values with tidy fields
CREATE OR REPLACE VIEW vw_monthly_series AS
SELECT
    f.date_id,
    d.year,
    d.month,
    d.is_month_start,
    s.series_key,
    s.series_id,
    s.series_name,
    s.unit,
    s.source,
    f.value,
    f.yoy_change,
    f.rolling_3m
FROM fact_series_values f
JOIN dim_series s ON s.series_key = f.series_key
JOIN dim_date d ON d.date_id = f.date_id
ORDER BY f.date_id, s.series_key;


-- 2) Pivot view: inflation vs wages side-by-side (Power BI loves this)
-- If you have annual CPIH rows, month will be 1; if monthly, it will be 1-12.
CREATE OR REPLACE VIEW vw_inflation_vs_wages AS
SELECT
    date_id,
    year,
    month,

    MAX(CASE WHEN series_key = 'cpih_l55o' THEN value END)      AS inflation_rate,
    MAX(CASE WHEN series_key = 'awe_kac3'  THEN value END)      AS wage_growth,

    MAX(CASE WHEN series_key = 'cpih_l55o' THEN yoy_change END) AS inflation_yoy_change,
    MAX(CASE WHEN series_key = 'awe_kac3'  THEN yoy_change END) AS wage_yoy_change,

    MAX(CASE WHEN series_key = 'cpih_l55o' THEN rolling_3m END) AS inflation_rolling_3m,
    MAX(CASE WHEN series_key = 'awe_kac3'  THEN rolling_3m END) AS wage_rolling_3m

FROM vw_monthly_series
GROUP BY date_id, year, month
ORDER BY date_id;


-- 3) Gap view: how far wages are from inflation (simple headline metric)
CREATE OR REPLACE VIEW vw_gap_summary AS
SELECT
    date_id,
    year,
    month,
    inflation_rate,
    wage_growth,
    (wage_growth - inflation_rate) AS wage_minus_inflation
FROM vw_inflation_vs_wages
WHERE inflation_rate IS NOT NULL
  AND wage_growth IS NOT NULL
ORDER BY date_id;


-- 4) Biggest gaps (top 20) for storytelling
CREATE OR REPLACE VIEW vw_biggest_gaps AS
SELECT *
FROM vw_gap_summary
ORDER BY ABS(wage_minus_inflation) DESC
LIMIT 20;
