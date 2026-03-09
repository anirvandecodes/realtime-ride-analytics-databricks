from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.table(comment="Ride status breakdown with percentage share for today.")
def gold_ride_status_breakdown():
    today = spark.read.table("silver_ride_events").filter(F.col("event_date") == F.current_date())
    total_window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    return (
        today.groupBy("status")
        .agg(F.count("ride_id").alias("ride_count"))
        .withColumn("pct_of_total",
            F.round(F.col("ride_count") * 100.0 / F.sum("ride_count").over(total_window), 1)
        )
        .orderBy(F.col("ride_count").desc())
    )
