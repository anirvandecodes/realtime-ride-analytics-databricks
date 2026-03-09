from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Driver performance leaderboard by city for today — rides, cancellations, rating, revenue.")
def gold_driver_leaderboard():
    today = spark.read.table("silver_ride_events").filter(F.col("event_date") == F.current_date())
    return (
        today.groupBy("driver_id", "driver_name", "city", "vehicle_type")
        .agg(
            F.count("ride_id").alias("total_rides"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_rides"),
            F.sum(F.col("is_cancelled").cast("int")).alias("cancelled_rides"),
            F.round(
                F.sum(F.col("is_cancelled").cast("int")) * 100.0 / F.count("ride_id"), 1
            ).alias("cancellation_rate_pct"),
            F.round(F.avg("driver_rating"), 2).alias("avg_rating"),
            F.round(
                F.sum(F.when(F.col("status") == "completed", F.col("final_fare")).otherwise(0)), 2
            ).alias("revenue_inr"),
        )
        .orderBy(F.col("cancelled_rides").desc())
    )
