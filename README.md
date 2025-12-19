# UK Cost of Living: Inflation vs Wage Growth (ONS)

This project analyses long-term UK inflation and wage growth trends using Office for National Statistics (ONS) time-series data.  
It demonstrates an **end-to-end analytics pipeline**, from automated data ingestion and transformation to SQL-based analysis and Power BI visualisation.

The objective is to understand how **real-terms wage pressure** has evolved over time and identify periods where earnings failed to keep pace with rising prices.

---

## Key Insights

- UK wage growth does not consistently keep pace with inflation, resulting in prolonged periods of real-terms income pressure.
- Significant negative real-terms wage gaps are observed during recessionary and economic shock periods.
- Post-pandemic years show heightened volatility between inflation and earnings growth, indicating increased economic uncertainty.

---

## Architecture Overview

This project follows a reproducible analytics pipeline:

1. **Extract**  
   - Automated download of CPIH inflation and Average Weekly Earnings (AWE) time-series data from the UK Office for National Statistics (ONS).

2. **Transform**  
   - Data cleaning and standardisation  
   - Feature engineering including year-on-year change and rolling averages  
   - Consolidation into a unified long-format analytical dataset

3. **Load**  
   - PostgreSQL analytical database  
   - Star-schema design with dimension and fact tables

4. **Analyse & Visualise**  
   - SQL analytical views for insight generation  
   - Power BI dashboards for trend comparison and real-terms gap analysis

---

## Data Sources

- **Office for National Statistics (ONS)**
  - CPIH Inflation Index  
  - Average Weekly Earnings (Total Pay, excluding arrears)

All datasets are publicly available and automatically downloaded during pipeline execution.

---

## Database Schema

The PostgreSQL database uses a simple analytical schema:

- **dim_series** – metadata for each economic time series  
- **dim_date** – calendar date dimension  
- **fact_series_values** – inflation and wage observations with derived metrics

---

## Example SQL Analysis

The following SQL query underpins the real-terms wage gap analysis by comparing inflation and wage growth over time:

```sql
SELECT
    d.date_id,
    d.year,
    d.month,
    i.value AS inflation_rate,
    w.value AS wage_growth,
    (w.value - i.value) AS wage_minus_inflation
FROM fact_series_values i
JOIN fact_series_values w
    ON i.date_id = w.date_id
JOIN dim_date d
    ON d.date_id = i.date_id
WHERE i.series_key = 'cpih_l55o'
  AND w.series_key = 'awe_kac3'
ORDER BY d.date_id;

---

## Power BI Dashboard

The Power BI report presents three analytical views:

1. **Inflation vs Wage Growth** – long-term comparison of consumer price inflation and earnings growth  
2. **Real-Terms Wage Gap** – wage growth minus inflation to assess changes in purchasing power  
3. **Largest Divergences** – periods with the most extreme positive or negative gaps  

These dashboards highlight sustained cost-of-living pressures and periods of economic stress in the UK.

---

## Project Structure

uk-cost-of-living-pipeline/
│
├── data/
│ ├── raw/ # Downloaded ONS CSV files
│ └── processed/ # Cleaned and transformed datasets
│
├── src/
│ ├── extract/ # Data extraction scripts
│ ├── transform/ # Data cleaning and feature engineering
│ └── load/ # PostgreSQL loading scripts
│
├── sql/
│ ├── 01_create_tables.sql
│ └── 02_analysis_views.sql
│
├── dashboards/ # Power BI files
├── logs/ # Pipeline logs
└── README.md

---

## Skills Demonstrated

- Python (pandas, numpy, logging)
- SQL (PostgreSQL, analytical views)
- ETL pipeline design and data modelling
- Power BI dashboard development
- Economic and time-series data analysis

---

## Future Improvements

- Automate scheduled data refreshes using Task Scheduler or cron
- Incorporate additional ONS series (energy prices, housing costs)
- Publish dashboards to Power BI Service for online access

---

## How to Run

1. **Clone the repository**
```bash
git clone https://github.com/Samson-tech-code/uk-cost-of-living-pipeline.git
cd uk-cost-of-living-pipeline


2. Create and activate a virtual environment

python -m venv .venv
.venv\Scripts\activate   # Windows


3. Install dependencies

pip install -r requirements.txt


4. Run the pipeline

python src/extract/download_ons_timeseries.py
python src/transform/transform_ons_timeseries.py
python src/load/load_to_postgres.py


5. Explore insights

Run SQL analysis in sql/02_analysis_views.sql

Open the Power BI dashboard from the /dashboards folder

---
#License

This project uses publicly available data from the UK Office for National Statistics (ONS) and is shared for educational and portfolio purposes only.
