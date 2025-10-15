from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ------------ Spark session ------------
spark = (
    SparkSession.builder
    .appName("WikiWatch-File")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

# ------------ Schema ------------
schema = StructType([
    StructField("meta", StructType([
        StructField("dt", StringType()),
        StructField("uri", StringType())
    ])),
    StructField("server_name", StringType()),
    StructField("wiki", StringType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("type", StringType()),
    StructField("length", StructType([
        StructField("old", IntegerType()),
        StructField("new", IntegerType())
    ])),
    StructField("comment", StringType()),
    StructField("namespace", IntegerType())
])

# ------------ Source stream ------------
src = (
    spark.readStream.schema(schema)
    .json("data/bronze")                                 # new files as they arrive
    .withColumn("ts", F.to_timestamp(F.col("meta.dt")))
    .withColumn("delta_bytes", F.col("length.new") - F.col("length.old"))
    .withColumn("project", F.coalesce(F.col("server_name"), F.col("wiki")))
    .withColumn("page_title", F.col("title"))
    .withWatermark("ts", "20 minutes")                   # generous watermark
)

# ------------ 1) Edits/min by project (tumbling 1m) ------------
by_proj = (
    src.groupBy(F.window("ts", "1 minute").alias("window"), F.col("project"))
       .agg(
           F.count(F.lit(1)).alias("edits"),
           F.approx_count_distinct("page_title").alias("unique_pages"),
           F.avg(F.when(F.col("bot") == True, 1).otherwise(0)).alias("bots_share"),
       )
)

# ------------ 2) Top pages (sliding 10m, step 1m) ------------
pages = (
    src.groupBy(F.window("ts", "10 minutes", "1 minute").alias("window"),
                F.col("project"), F.col("page_title"))
       .count().withColumnRenamed("count", "edits")
)

# ------------ 3) Spike detector (1m vs 10m baseline) ------------
# 1-minute counts per project
edits_1m = (
    src.groupBy(F.window("ts", "1 minute").alias("w1"), F.col("project"))
       .count().withColumnRenamed("count", "edits_1m")
)

# 10-minute counts per project, sliding each minute so ends align with w1.end
edits_10m = (
    src.groupBy(F.window("ts", "10 minutes", "1 minute").alias("w10"), F.col("project"))
       .count().withColumnRenamed("count", "edits_10m")
)

# Join on project and aligned minute tick (w1.end == w10.end)
spikes = (
    edits_1m.alias("a")
    .join(
        edits_10m.alias("b"),
        on=[
            F.col("a.project") == F.col("b.project"),
            F.col("a.w1.end") == F.col("b.w10.end")
        ],
        how="inner",
    )
    .select(
        F.col("a.w1").alias("window"),
        F.col("a.project").alias("project"),
        F.col("edits_1m"),
        F.col("edits_10m"),
        # compare the 1-minute count to the 10-minute-per-minute average
        (F.col("edits_1m") / F.greatest(F.col("edits_10m") / F.lit(10.0), F.lit(1e-6))).alias("ratio")
    )
    .where((F.col("edits_10m") >= 5) & (F.col("ratio") >= 2.0))   # filter to real bursts
)

# Optional normalized score for UI (Poisson-ish z-score feel)
spikes = spikes.withColumn(
    "score",
    (F.col("edits_1m") - (F.col("edits_10m") / 10.0)) /
    F.sqrt(F.greatest(F.col("edits_10m") / 10.0, F.lit(1.0)))
)

# ------------ 4) Cross-language pages (>= 2 projects in 10m) ------------
pages10 = (
    src.groupBy(F.window("ts", "10 minutes", "1 minute").alias("window"),
                F.col("project"), F.col("page_title"))
       .count()
)
cross = (
    pages10.groupBy("window", "page_title")
           .agg(
               F.collect_set("project").alias("projects"),
               F.sum("count").alias("total_edits")
           )
           .withColumn("n_projects", F.size("projects"))
           .filter(F.col("n_projects") >= 2)
)

# ------------ Sinks (use fresh checkpoints) ------------
(by_proj.writeStream.format("parquet")
   .option("path", "data/gold/by_project")
   .option("checkpointLocation", "data/checkpoints/by_project_v2")
   .outputMode("append")
   .start())

(pages.writeStream.format("parquet")
   .option("path", "data/gold/top_pages")
   .option("checkpointLocation", "data/checkpoints/top_pages_v2")
   .outputMode("append")
   .start())

(spikes.writeStream.format("parquet")
   .option("path", "data/gold/spikes_v2")
   .option("checkpointLocation", "data/checkpoints/spikes_v2")
   .outputMode("append")
   .start())

(cross.writeStream.format("parquet")
   .option("path", "data/gold/crosslang_v2")
   .option("checkpointLocation", "data/checkpoints/crosslang_v2")
   .outputMode("append")
   .start())

spark.streams.awaitAnyTermination()
