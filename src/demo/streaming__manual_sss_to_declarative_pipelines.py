# Databricks notebook source
# MAGIC %md
# MAGIC # Why Spark Declarative Pipelines? Declarative vs Manual Spark Structured Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Manual Way — Plain Spark Structured Streaming
# MAGIC
# MAGIC Here's what you'd write to build just the Bronze → Silver step yourself.

# COMMAND ----------

# --- THE MANUAL WAY (do NOT run — illustrative only) ---

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

LANDING_PATH    = "/Volumes/workspace/realtime/landing"
BRONZE_PATH     = "/tmp/demo/bronze"
SILVER_PATH     = "/tmp/demo/silver"
CHECKPOINT_BRONZE = "/tmp/demo/checkpoints/bronze"
CHECKPOINT_SILVER = "/tmp/demo/checkpoints/silver"

SCHEMA = StructType([
    StructField("ride_id",          StringType(),  True),
    StructField("event_time",       StringType(),  True),
    StructField("status",           StringType(),  True),
    StructField("city",             StringType(),  True),
    StructField("vehicle_type",     StringType(),  True),
    StructField("driver_id",        StringType(),  True),
    StructField("driver_rating",    DoubleType(),  True),
    StructField("distance_km",      DoubleType(),  True),
    StructField("duration_mins",    IntegerType(), True),
    StructField("final_fare",       DoubleType(),  True),
    StructField("is_peak_hour",     BooleanType(), True),
])

# Step 1 — Bronze: read from volume, write to Delta
bronze_query = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA)
        .load(LANDING_PATH)
        .withColumn("_ingest_time", F.current_timestamp())
        .writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_BRONZE)   # YOU manage this
        .outputMode("append")
        .start(BRONZE_PATH)
)

# Step 2 — Silver: read from Bronze, filter, enrich, write to Delta
def process_silver(batch_df, batch_id):
    # YOU write custom error handling per micro-batch
    try:
        cleaned = (
            batch_df
            .filter(F.col("ride_id").isNotNull())
            .filter(F.col("final_fare") > 0)
            .filter(F.col("distance_km").between(0.5, 50))
            .filter(F.col("driver_rating").between(1.0, 5.0))
            .withColumn("event_time",  F.to_timestamp("event_time"))
            .withColumn("event_date",  F.to_date("event_time"))
            .withColumn("event_hour",  F.hour("event_time"))
            .withColumn("is_cancelled",
                F.col("status").isin(
                    "cancelled_by_user", "cancelled_by_driver", "no_driver_found"
                ).cast("boolean"))
        )
        cleaned.write.format("delta").mode("append").save(SILVER_PATH)
    except Exception as e:
        print(f"Batch {batch_id} failed: {e}")  # YOU handle failures
        raise

silver_query = (
    spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
        .writeStream
        .foreachBatch(process_silver)
        .option("checkpointLocation", CHECKPOINT_SILVER)   # YOU manage this
        .start()
)

# YOU must keep both queries alive, handle restarts, monitor them
bronze_query.awaitTermination()
silver_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What you just took on with that code ☝️
# MAGIC
# MAGIC | Responsibility | You must handle it yourself |
# MAGIC |---|---|
# MAGIC | Checkpoint management | Create paths, ensure they don't corrupt on restart |
# MAGIC | Schema enforcement | Write your own filter logic, no tracking of violations |
# MAGIC | Error handling | `try/except` in every `foreachBatch` |
# MAGIC | Retries on failure | Re-run the whole job, re-apply logic |
# MAGIC | Lineage | None — no way to see what feeds what |
# MAGIC | Monitoring | Parse logs manually or wire up your own metrics |
# MAGIC | Table registration | Register in Unity Catalog separately |
# MAGIC | Data quality metrics | Track violations yourself |
# MAGIC
# MAGIC And this is just **2 tables**. A real pipeline has 10–15.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## The Spark Declarative Pipelines Way
# MAGIC
# MAGIC Same Bronze → Silver logic. Watch how much disappears.

# COMMAND ----------

# --- THE SPARK DECLARATIVE PIPELINES WAY ---

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

SCHEMA = StructType([
    StructField("ride_id",          StringType(),  True),
    StructField("event_time",       StringType(),  True),
    StructField("status",           StringType(),  True),
    StructField("city",             StringType(),  True),
    StructField("vehicle_type",     StringType(),  True),
    StructField("driver_id",        StringType(),  True),
    StructField("driver_rating",    DoubleType(),  True),
    StructField("distance_km",      DoubleType(),  True),
    StructField("duration_mins",    IntegerType(), True),
    StructField("final_fare",       DoubleType(),  True),
    StructField("is_peak_hour",     BooleanType(), True),
])

# Bronze — 6 lines, no checkpoint path, no format wiring
@dp.table(comment="Raw ride events from landing volume.")
def bronze_ride_events():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(SCHEMA)
            .load("/Volumes/workspace/realtime/landing")
            .withColumn("_ingest_time", F.current_timestamp())
    )

# Silver — quality rules declared, not coded
@dp.table(comment="Cleaned and enriched ride events.")
@dp.expect_or_drop("valid_ride_id",  "ride_id IS NOT NULL")
@dp.expect_or_drop("valid_fare",     "final_fare > 0")
@dp.expect_or_drop("valid_distance", "distance_km BETWEEN 0.5 AND 50")
@dp.expect_or_drop("valid_rating",   "driver_rating BETWEEN 1.0 AND 5.0")
def silver_ride_events():
    return (
        spark.readStream.table("bronze_ride_events")
            .withColumn("event_time",   F.to_timestamp("event_time"))
            .withColumn("event_date",   F.to_date("event_time"))
            .withColumn("event_hour",   F.hour("event_time"))
            .withColumn("is_cancelled",
                F.col("status").isin(
                    "cancelled_by_user", "cancelled_by_driver", "no_driver_found"
                ).cast("boolean"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### What Spark Declarative Pipelines gave you for free ✅
# MAGIC
# MAGIC | What you needed | Manual Streaming | Spark Declarative Pipelines |
# MAGIC |---|---|---|
# MAGIC | Checkpoint management | You create & manage paths | ✅ Automatic |
# MAGIC | Schema enforcement | Write filter code | ✅ `@dp.expect_or_drop` — declared |
# MAGIC | Violation tracking | Log it yourself | ✅ Built-in metrics per rule |
# MAGIC | Retry on failure | Re-run job manually | ✅ Auto-retry with backoff |
# MAGIC | Data lineage graph | Not available | ✅ Visual DAG in the UI |
# MAGIC | Unity Catalog registration | Separate DDL step | ✅ Automatic |
# MAGIC | Pipeline monitoring | Parse logs manually | ✅ Event log + UI |
# MAGIC | Adding a new table | New query + checkpoint + wiring | ✅ Add one `@dp.table` function |
# MAGIC
# MAGIC **You describe what you want. Spark Declarative Pipelines figures out how to run it.**
