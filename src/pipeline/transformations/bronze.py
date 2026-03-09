from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType,
)

KAFKA_BOOTSTRAP = spark.conf.get("kafka_bootstrap_servers")
KAFKA_TOPIC     = spark.conf.get("kafka_topic",        "ride-events")
SECRET_SCOPE    = spark.conf.get("kafka_secret_scope", "confluent-kafka")

_api_key    = dbutils.secrets.get(scope=SECRET_SCOPE, key="api-key")
_api_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key="api-secret")

KAFKA_JAAS = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
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


@dp.table(
    comment="Raw ride events ingested from Confluent Kafka. Append-only, no transformations.",
    table_properties={"quality": "bronze"},
)
def bronze_ride_events():
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers",                     KAFKA_BOOTSTRAP)
            .option("kafka.security.protocol",                     "SASL_SSL")
            .option("kafka.sasl.mechanism",                        "PLAIN")
            .option("kafka.sasl.jaas.config",                      KAFKA_JAAS)
            .option("kafka.ssl.endpoint.identification.algorithm", "https")
            .option("subscribe",                                   KAFKA_TOPIC)
            .option("startingOffsets",  "latest")
            .option("failOnDataLoss",   "false")
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
    )
