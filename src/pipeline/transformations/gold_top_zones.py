from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Top 10 zones by revenue for completed rides today.")
def gold_top_zones():
    today = spark.read.table("silver_ride_events").filter(F.col("event_date") == F.current_date())
    return (
        today.filter(F.col("status") == "completed")
        .groupBy("zone_id", "city")
        .agg(
            F.round(F.sum("final_fare"), 2).alias("total_revenue_inr"),
            F.count("ride_id").alias("completed_rides"),
            F.round(F.avg("final_fare"), 2).alias("avg_fare_inr"),
            F.round(F.avg("distance_km"), 1).alias("avg_distance_km"),
        )
        .orderBy(F.col("total_revenue_inr").desc())
        .limit(10)
    )
