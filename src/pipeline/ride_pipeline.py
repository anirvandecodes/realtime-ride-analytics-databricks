# Databricks notebook source
# MAGIC %md
# MAGIC # Ride Analytics — Spark Structured Streaming Pipeline
# MAGIC
# MAGIC **Medallion Architecture:**
# MAGIC ```
# MAGIC Confluent Kafka (ride-events topic)
# MAGIC   └─► Bronze  — Kafka → Delta   (append, 5-second micro-batches)
# MAGIC         └─► Silver — Delta → Delta  (filter + enrich, append)
# MAGIC               └─► Gold  — foreachBatch → Delta  (full-table aggregations, 30-second refresh)
# MAGIC ```
# MAGIC
# MAGIC Run this notebook as a **Databricks Job task** — it streams indefinitely until stopped.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("kafka_bootstrap_servers", "", "Kafka Bootstrap Servers")
dbutils.widgets.text("kafka_topic",             "ride-events",     "Kafka Topic")
dbutils.widgets.text("kafka_secret_scope",      "confluent-kafka", "Secret Scope")

KAFKA_BOOTSTRAP = dbutils.widgets.get("kafka_bootstrap_servers")
KAFKA_TOPIC     = dbutils.widgets.get("kafka_topic")
SECRET_SCOPE    = dbutils.widgets.get("kafka_secret_scope")

CATALOG         = "workspace"
SCHEMA          = "realtime"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType
)

_api_key    = dbutils.secrets.get(scope=SECRET_SCOPE, key="api-key")
_api_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key="api-secret")

KAFKA_JAAS = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{_api_key}" password="{_api_secret}";'
)

RIDE_SCHEMA = StructType([
    StructField("ride_id",             StringType(),  nullable=True),
    StructField("event_time",          StringType(),  nullable=True),
    StructField("status",              StringType(),  nullable=True),
    StructField("city",                StringType(),  nullable=True),
    StructField("pickup_area",         StringType(),  nullable=True),
    StructField("drop_area",           StringType(),  nullable=True),
    StructField("vehicle_type",        StringType(),  nullable=True),
    StructField("driver_id",           StringType(),  nullable=True),
    StructField("driver_name",         StringType(),  nullable=True),
    StructField("driver_rating",       DoubleType(),  nullable=True),
    StructField("rider_id",            StringType(),  nullable=True),
    StructField("distance_km",         DoubleType(),  nullable=True),
    StructField("duration_mins",       IntegerType(), nullable=True),
    StructField("base_fare",           DoubleType(),  nullable=True),
    StructField("surge_multiplier",    DoubleType(),  nullable=True),
    StructField("final_fare",          DoubleType(),  nullable=True),
    StructField("payment_method",      StringType(),  nullable=True),
    StructField("cancellation_reason", StringType(),  nullable=True),
    StructField("zone_id",             StringType(),  nullable=True),
    StructField("is_peak_hour",        BooleanType(), nullable=True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer — Kafka → Delta

# COMMAND ----------

bronze_query = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",  KAFKA_BOOTSTRAP)
        .option("kafka.security.protocol",  "SASL_SSL")
        .option("kafka.sasl.mechanism",     "PLAIN")
        .option("kafka.sasl.jaas.config",   KAFKA_JAAS)
        .option("subscribe",                KAFKA_TOPIC)
        .option("startingOffsets",          "latest")
        .option("failOnDataLoss",           "false")
        .load()
        .withColumn("value_str", F.col("value").cast("string"))
        .withColumn("ride",      F.from_json(F.col("value_str"), RIDE_SCHEMA))
        .select(
            F.col("ride.*"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .withColumn("_ingest_time", F.current_timestamp())
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
        .trigger(processingTime="5 seconds")
        .toTable(f"`{CATALOG}`.`{SCHEMA}`.bronze_ride_events")
)

print(f"Bronze stream started — ID: {bronze_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer — Clean & Validate

# COMMAND ----------

def _transform_silver(df):
    return (
        df
        .filter(F.col("ride_id").isNotNull())
        .filter(F.col("final_fare") > 0)
        .filter(F.col("distance_km").between(0.5, 50))
        .filter(F.col("driver_rating").between(1.0, 5.0))
        .filter(F.col("status").isin(
            "completed", "cancelled_by_user", "cancelled_by_driver",
            "no_driver_found", "in_progress"
        ))
        .filter(F.col("vehicle_type").isin("Bike", "Auto", "Mini", "Sedan", "SUV"))
        .withColumn("event_time",    F.to_timestamp("event_time"))
        .withColumn("event_date",    F.to_date("event_time"))
        .withColumn("event_hour",    F.hour("event_time"))
        .withColumn("event_minute",  F.minute("event_time"))
        .withColumn("day_of_week",   F.dayofweek("event_time"))
        .withColumn("is_weekend",    F.dayofweek("event_time").isin([1, 7]).cast("boolean"))
        .withColumn("final_fare",    F.col("final_fare").cast(DoubleType()))
        .withColumn("base_fare",     F.col("base_fare").cast(DoubleType()))
        .withColumn("distance_km",   F.col("distance_km").cast(DoubleType()))
        .withColumn("duration_mins", F.col("duration_mins").cast(IntegerType()))
        .withColumn("is_cancelled",
            F.col("status").isin(
                "cancelled_by_user", "cancelled_by_driver", "no_driver_found"
            ).cast("boolean")
        )
        .drop("kafka_partition", "kafka_offset")
    )


silver_query = (
    spark.readStream
        .format("delta")
        .table(f"`{CATALOG}`.`{SCHEMA}`.bronze_ride_events")
        .transform(_transform_silver)
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver")
        .trigger(processingTime="5 seconds")
        .toTable(f"`{CATALOG}`.`{SCHEMA}`.silver_ride_events")
)

print(f"Silver stream started — ID: {silver_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer — Dashboard Aggregations (foreachBatch)

# COMMAND ----------

def _update_gold_tables(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    silver = spark.table(f"`{CATALOG}`.`{SCHEMA}`.silver_ride_events")
    today  = silver.filter(F.col("event_date") == F.current_date())

    (today
        .agg(
            F.count("ride_id").alias("total_rides"),
            F.sum(
                F.when(F.col("status") == "completed", F.col("final_fare")).otherwise(0)
            ).alias("total_revenue_inr"),
            F.round(
                F.sum(F.col("is_cancelled").cast("int")) * 100.0 / F.count("ride_id"), 1
            ).alias("cancellation_rate_pct"),
            F.countDistinct("driver_id").alias("active_drivers"),
            F.round(F.avg(
                F.when(F.col("status") == "completed", F.col("final_fare"))
            ), 2).alias("avg_fare_inr"),
            F.max("_ingest_time").alias("last_updated"),
        )
        .write.mode("overwrite")
        .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.gold_live_kpis")
    )

    (silver
        .filter(F.col("status") == "completed")
        .filter(F.col("event_time") >= F.expr("current_timestamp() - INTERVAL 60 MINUTES"))
        .withColumn("minute_bucket", F.date_trunc("minute", "event_time"))
        .groupBy("minute_bucket")
        .agg(
            F.round(F.sum("final_fare"), 2).alias("revenue_inr"),
            F.count("ride_id").alias("completed_rides"),
        )
        .orderBy("minute_bucket")
        .write.mode("overwrite")
        .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.gold_revenue_per_minute")
    )

    (today
        .groupBy("city")
        .agg(
            F.count("ride_id").alias("total_rides"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_rides"),
            F.round(F.sum(
                F.when(F.col("status") == "completed", F.col("final_fare")).otherwise(0)
            ), 2).alias("revenue_inr"),
            F.round(F.avg(
                F.when(F.col("status") == "completed", F.col("final_fare"))
            ), 2).alias("avg_fare_inr"),
        )
        .orderBy(F.col("total_rides").desc())
        .write.mode("overwrite")
        .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.gold_rides_by_city")
    )

    total_today = today.count()
    (today
        .groupBy("status")
        .agg(F.count("ride_id").alias("ride_count"))
        .withColumn("pct_of_total",
            F.round(F.col("ride_count") * 100.0 / total_today, 1)
        )
        .orderBy(F.col("ride_count").desc())
        .write.mode("overwrite")
        .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.gold_ride_status_breakdown")
    )

    (today
        .filter(F.col("status") == "completed")
        .groupBy("zone_id", "city")
        .agg(
            F.round(F.sum("final_fare"), 2).alias("total_revenue_inr"),
            F.count("ride_id").alias("completed_rides"),
            F.round(F.avg("final_fare"), 2).alias("avg_fare_inr"),
            F.round(F.avg("distance_km"), 1).alias("avg_distance_km"),
        )
        .orderBy(F.col("total_revenue_inr").desc())
        .limit(10)
        .write.mode("overwrite")
        .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.gold_top_zones")
    )

    (today
        .groupBy("event_hour", "is_peak_hour")
        .agg(
            F.count("ride_id").alias("total_rides"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_rides"),
            F.round(F.sum(
                F.when(F.col("status") == "completed", F.col("final_fare")).otherwise(0)
            ), 2).alias("revenue_inr"),
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge"),
        )
        .orderBy("event_hour")
        .write.mode("overwrite")
        .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.gold_hourly_demand")
    )


gold_query = (
    spark.readStream
        .format("delta")
        .table(f"`{CATALOG}`.`{SCHEMA}`.silver_ride_events")
        .writeStream
        .foreachBatch(_update_gold_tables)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold")
        .start()
)

print(f"Gold stream started  — ID: {gold_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## All Streams Running
# MAGIC Stop the notebook (interrupt) to halt the pipeline.

# COMMAND ----------

print("Stream status:")
print(f"  Bronze : {bronze_query.status['message']}")
print(f"  Silver : {silver_query.status['message']}")
print(f"  Gold   : {gold_query.status['message']}")

spark.streams.awaitAnyTermination()
