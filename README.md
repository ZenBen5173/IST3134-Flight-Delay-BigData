# IST3134 Big Data Analytics — US Flight Delay Analysis (2015–2024)

Group assignment comparing a single-machine approach (Python + pandas) against a
cloud Big Data approach (Apache Spark / PySpark on AWS EMR) on the same analysis of
US domestic flight delays.

## Question
Over 2015–2024, which airports, routes, carriers and times of day had the worst
flight delays, and how did the mix of delay causes shift over the decade?
(A flight is "delayed" if its arrival delay is 15 minutes or more.)

## Dataset
US Bureau of Transportation Statistics — "Reporting Carrier On-Time Performance (1987-present)".
Free, no login. Source: https://www.transtats.bts.gov
- One file per month; 2015–2024 = 120 monthly files, ~62 million flights, ~110 columns.
- The raw data is NOT stored in this repo (too large for GitHub). Download the monthly
  files from the source above (each file: PREZIP archive named
  `On_Time_Reporting_Carrier_On_Time_Performance_1987_present_<YEAR>_<MONTH>.zip`).

## Repository structure
- `Code/` — Jupyter notebooks
  - `01_local_1month … 04_local_10year.ipynb` — pandas baseline (single machine)
  - `05_spark_1month … 08_spark_10year.ipynb` — PySpark (local mode)
- `AWS/` — cloud (Big Data) Spark jobs
  - `2_spark_emr.py` — Spark job for a single run on EMR (reads from S3)
  - `3_spark_scaling.py` — Spark job that runs all four scopes and records runtimes
- `Output/` — results and figures
  - `1month/ 1year/ 5year/ 10year/` — result tables (CSV), charts and an HTML report per run
  - `comparison/` — pandas-vs-Spark runtime, memory and speed-up charts
  - `aws_*.png` — screenshots evidencing the AWS EMR run
- `Data/Cleaned/` — small result tables (CSV)
- `IST3134_Report.docx` / `IST3134_Report.pdf` — the full written report

## How to reproduce
1. Download the monthly data files from the BTS source above into `Data/Raw/`
   (and upload the same files to an S3 bucket for the AWS run).
2. Local (pandas): open the `Code/0x_local_*` notebooks and Run All
   (`pip install pandas matplotlib`).
3. Local (Spark): open the `Code/0x_spark_*` notebooks (`pip install pyspark pandas matplotlib`, Java 17 required).
4. Cloud (Spark on AWS): create an EMR cluster (Spark), upload `AWS/3_spark_scaling.py`
   to S3, then run it as a Spark step with the bucket name as the argument.

## Key result
Same algorithm (a group-by / map-reduce aggregation) both ways. At 10 years (~62M rows)
the pandas baseline needed 38.4 GB of memory and 479 s, while PySpark on a 3-node AWS
EMR cluster finished in 250 s within a bounded ~12 GB per container — showing why a
distributed cloud platform is needed as the data grows.
