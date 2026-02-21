# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC Creates the Unity Catalog structure needed for the ride analytics platform,
# MAGIC and walks you through setting up Confluent Kafka credentials in Databricks Secrets.
# MAGIC
# MAGIC Run this notebook **once** before deploying the pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "workspace"
SCHEMA  = "realtime"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Create Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Catalog '{CATALOG}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Schema '{CATALOG}.{SCHEMA}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Create Checkpoints Volume

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")
print(f"Volume '/Volumes/{CATALOG}/{SCHEMA}/checkpoints' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Confluent Kafka Secrets
# MAGIC
# MAGIC Store your Confluent Cloud API credentials in a **Databricks Secret Scope**.
# MAGIC
# MAGIC Run the following commands **once in your terminal** (requires Databricks CLI):
# MAGIC
# MAGIC ```bash
# MAGIC # Create the secret scope
# MAGIC databricks secrets create-scope confluent-kafka
# MAGIC
# MAGIC # Add your Confluent Cloud API key and secret
# MAGIC databricks secrets put-secret confluent-kafka api-key    --string-value "<YOUR_API_KEY>"
# MAGIC databricks secrets put-secret confluent-kafka api-secret --string-value "<YOUR_API_SECRET>"
# MAGIC ```
# MAGIC
# MAGIC After running the above, verify:
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets list-secrets confluent-kafka
# MAGIC ```

# COMMAND ----------

api_key    = dbutils.secrets.get(scope="confluent-kafka", key="api-key")
api_secret = dbutils.secrets.get(scope="confluent-kafka", key="api-secret")
print(f"api-key    = {api_key}")
print(f"api-secret = {api_secret}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Verify

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
