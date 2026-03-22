from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Ride status counts per day. Query pct_of_total as: ROUND(ride_count * 100.0 / SUM(ride_count) OVER (PARTITION BY event_date), 1).")
def gold_ride_status_breakdown():
    return (
        spark.read.table("silver_ride_events")
        .groupBy("event_date", "status")
        .agg(F.count("ride_id").alias("ride_count"))
    )
