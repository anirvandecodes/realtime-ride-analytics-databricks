from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(comment="Rides and revenue by city for today.")
def gold_rides_by_city():
    return (
        spark.read.table("silver_ride_events")
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
    )
