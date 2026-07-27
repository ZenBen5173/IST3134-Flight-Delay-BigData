# =====================================================================
# Spark job for EMR - reads ALL files in s3://<bucket>/data/ from S3
# Run:  spark-submit 2_spark_emr.py <BUCKET_NAME>
# (Works with whatever years are uploaded - currently 2015-2018.)
# =====================================================================
import sys, time
from pyspark.sql import SparkSession, functions as F

BUCKET = sys.argv[1] if len(sys.argv) > 1 else "ist3134-flights-teoze-2026"
DATA = f"s3://{BUCKET}/data/"      # reads every .csv.gz in the folder

spark = SparkSession.builder.appName("flight-delays-emr").getOrCreate()
print("Reading from:", DATA)

t0 = time.perf_counter()
raw = (spark.read.option("header", True).option("quote", '"').option("escape", '"').csv(DATA))
df = raw.select(
    F.col("Year").cast("int").alias("Year"),
    F.col("Reporting_Airline").alias("carrier"),
    F.col("Origin"), F.col("Dest"),
    F.col("CRSDepTime").cast("int").alias("crs"),
    F.col("ArrDelay").cast("double").alias("arr"),
    F.col("Cancelled").cast("double").alias("canc"),
    F.col("CarrierDelay").cast("double"), F.col("WeatherDelay").cast("double"),
    F.col("NASDelay").cast("double"), F.col("SecurityDelay").cast("double"),
    F.col("LateAircraftDelay").cast("double"))
df = df.filter((F.col("canc") != 1) | F.col("canc").isNull())
df = df.withColumn("delayed", F.when(F.col("arr") >= 15, 1).otherwise(0))
df = df.withColumn("dep_hour", F.least(F.greatest((F.col("crs")/100).cast("int"), F.lit(0)), F.lit(23)))
df = df.withColumn("route", F.concat_ws("-", "Origin", "Dest")).cache()
n = df.count()

def agg(g):
    return df.groupBy(g).agg(F.count(F.lit(1)).alias("flights"),
                             F.avg("arr").alias("avg_arr_delay"),
                             (F.avg("delayed")*100).alias("pct_delayed"))

by_carrier = agg("carrier").orderBy(F.desc("pct_delayed"))
by_airport = agg("Origin").filter("flights >= 1000").orderBy(F.desc("pct_delayed"))
by_route   = agg("route").filter("flights >= 500").orderBy(F.desc("pct_delayed"))
by_hour    = agg("dep_hour").orderBy("dep_hour")
causes = ["CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay"]
cause_by_year = df.groupBy("Year").agg(*[F.sum(c).alias(c) for c in causes]).orderBy("Year")

for name, d in [("by_carrier",by_carrier),("by_airport",by_airport),("by_route",by_route),
                ("by_hour",by_hour),("cause_by_year",cause_by_year)]:
    d.coalesce(1).write.mode("overwrite").option("header",True).csv(f"s3://{BUCKET}/output/{name}")

secs = time.perf_counter() - t0
print("==================================================")
print(f"ROWS = {n:,}    RUNTIME = {secs:.1f}s    (EMR / Spark cluster)")
print("Worst 5 carriers by % delayed:")
by_carrier.show(5)
print("Results written to s3://%s/output/" % BUCKET)
print("==================================================")
spark.stop()
