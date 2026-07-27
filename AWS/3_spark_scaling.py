# EMR Spark job: runs all four scopes (1 month / 1 year / 5 year / 10 year),
# reads .csv.gz from s3://<bucket>/data/, writes results + a runtime summary to S3.
# Run:  spark-submit 3_spark_scaling.py <BUCKET_NAME>
import sys, time
from pyspark.sql import SparkSession, functions as F
BUCKET = sys.argv[1] if len(sys.argv) > 1 else "ist3134-flights-teoze-2026"
spark = SparkSession.builder.appName("flight-delays-scaling").getOrCreate()
SCOPES = {
    "1month": [(2024, [1])],
    "1year":  [(2024, list(range(1,13)))],
    "5year":  [(y, list(range(1,13))) for y in range(2020, 2025)],
    "10year": [(y, list(range(1,13))) for y in range(2015, 2025)],
}
def paths(spec):
    return [f"s3://{BUCKET}/data/ontime_{y}_{m}.csv.gz" for y, ms in spec for m in ms]
def analyze(tag, spec):
    t0 = time.perf_counter()
    raw = spark.read.option("header", True).option("quote", '"').option("escape", '"').csv(paths(spec))
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
    cby = df.groupBy("Year").agg(*[F.sum(c).alias(c) for c in causes]).orderBy("Year")
    for name, d in [("by_carrier",by_carrier),("by_airport",by_airport),("by_route",by_route),("by_hour",by_hour),("cause_by_year",cby)]:
        d.coalesce(1).write.mode("overwrite").option("header",True).csv(f"s3://{BUCKET}/output_{tag}/{name}")
    secs = time.perf_counter() - t0
    df.unpersist()
    print(f">>> SCOPE {tag}: rows={n} runtime={secs:.1f}s")
    return (tag, int(n), round(secs, 1))
results = [analyze(tag, spec) for tag, spec in SCOPES.items()]
summ = spark.createDataFrame(results, ["scope", "rows", "runtime_s"])
summ.coalesce(1).write.mode("overwrite").option("header", True).csv(f"s3://{BUCKET}/scaling_summary")
for r in results: print(r)
spark.stop()
