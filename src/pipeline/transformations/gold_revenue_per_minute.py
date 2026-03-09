from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Completed ride revenue aggregated per minute over the last 60 minutes.")
def gold_revenue_per_minute():
    return (
        spark.read.table("silver_ride_events")
        .filter(F.col("status") == "completed")
        .filter(F.col("event_time") >= F.expr("current_timestamp() - INTERVAL 60 MINUTES"))
        .withColumn("minute_bucket", F.date_trunc("minute", "event_time"))
        .groupBy("minute_bucket")
        .agg(
            F.round(F.sum("final_fare"), 2).alias("revenue_inr"),
            F.count("ride_id").alias("completed_rides"),
        )
        .orderBy("minute_bucket")
    )
