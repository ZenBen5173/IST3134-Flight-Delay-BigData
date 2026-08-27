# US Flight Delay Analysis: one laptop vs. a Spark cluster

**62 million flights. One laptop running pandas, versus a Spark cluster on AWS, doing the
exact same analysis.** At what point does "just use a bigger machine" stop working, and you
actually need the cluster? We built both and measured it.

The analysis itself: over 2015 to 2024, which airports, routes, carriers, and times of day had
the worst US flight delays, and how did the mix of delay causes shift across the decade? (A
flight counts as delayed if it arrives 15 minutes or more late.)

Group assignment for IST3134 Big Data Analytics.

## The headline result

Same algorithm both ways: a group-by / map-reduce aggregation. At the full 10-year scale
(~62M rows):

- **pandas on one machine** needed **38.4 GB of memory** and **479 seconds**.
- **PySpark on a 3-node AWS EMR cluster** finished in **250 seconds**, staying within a bounded
  ~12 GB per container.

That gap is the whole point: as the data grows, a single machine hits a memory wall that a
distributed platform simply does not, which is exactly when the cluster earns its keep.

---

## Under the hood

### The dataset

US Bureau of Transportation Statistics, "Reporting Carrier On-Time Performance (1987-present)".
Free, no login, from [transtats.bts.gov](https://www.transtats.bts.gov).

- One file per month; 2015 to 2024 is 120 monthly files, roughly 62 million flights and ~110
  columns.
- The raw data is **not** stored in this repo (too large for GitHub). Download the monthly
  files from the source above (each is a PREZIP archive named
  `On_Time_Reporting_Carrier_On_Time_Performance_1987_present_<YEAR>_<MONTH>.zip`).

### Repository structure

- `Code/` Jupyter notebooks
  - `01_local_1month … 04_local_10year.ipynb`: pandas baseline (single machine)
  - `05_spark_1month … 08_spark_10year.ipynb`: PySpark (local mode)
- `AWS/` cloud (Big Data) Spark jobs
  - `2_spark_emr.py`: Spark job for a single run on EMR (reads from S3)
  - `3_spark_scaling.py`: Spark job that runs all four scopes and records runtimes
- `Output/` results and figures
  - `1month/ 1year/ 5year/ 10year/`: result tables (CSV), charts, and an HTML report per run
  - `comparison/`: pandas-vs-Spark runtime, memory, and speed-up charts
  - `aws_*.png`: screenshots evidencing the AWS EMR run
- `Data/Cleaned/`: small result tables (CSV)
- `IST3134_Report.docx` / `IST3134_Report.pdf`: the full written report

### How to reproduce

1. Download the monthly data files from the BTS source above into `Data/Raw/` (and upload the
   same files to an S3 bucket for the AWS run).
2. Local (pandas): open the `Code/0x_local_*` notebooks and Run All (`pip install pandas matplotlib`).
3. Local (Spark): open the `Code/0x_spark_*` notebooks (`pip install pyspark pandas matplotlib`,
   Java 17 required).
4. Cloud (Spark on AWS): create an EMR cluster (Spark), upload `AWS/3_spark_scaling.py` to S3,
   then run it as a Spark step with the bucket name as the argument.
