from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Live operational KPIs per day: rides, revenue, cancellation rate, active drivers.")
def gold_live_kpis():
    return spark.read.table("silver_ride_events").groupBy("event_date").agg(
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
