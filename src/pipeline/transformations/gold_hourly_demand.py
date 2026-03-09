from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Hourly demand, revenue, and surge pricing patterns for today.")
def gold_hourly_demand():
    today = spark.read.table("silver_ride_events").filter(F.col("event_date") == F.current_date())
    return (
        today.groupBy("event_hour", "is_peak_hour")
        .agg(
            F.count("ride_id").alias("total_rides"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_rides"),
            F.round(F.sum(
                F.when(F.col("status") == "completed", F.col("final_fare")).otherwise(0)
            ), 2).alias("revenue_inr"),
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge"),
        )
        .orderBy("event_hour")
    )
