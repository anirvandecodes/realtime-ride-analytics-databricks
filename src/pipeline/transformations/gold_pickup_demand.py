from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Ride demand by pickup area and city for today — identifies hot zones needing driver supply.")
def gold_pickup_demand():
    today = spark.read.table("silver_ride_events").filter(F.col("event_date") == F.current_date())
    return (
        today.groupBy("city", "pickup_area", "zone_id")
        .agg(
            F.count("ride_id").alias("total_rides"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_rides"),
            F.sum(F.col("is_cancelled").cast("int")).alias("cancelled_rides"),
            F.round(
                F.sum(F.col("is_cancelled").cast("int")) * 100.0 / F.count("ride_id"), 1
            ).alias("cancellation_rate_pct"),
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge"),
            F.round(
                F.sum(F.when(F.col("status") == "completed", F.col("final_fare")).otherwise(0)), 2
            ).alias("revenue_inr"),
        )
        .orderBy(F.col("total_rides").desc())
    )
