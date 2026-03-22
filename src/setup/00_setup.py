# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC Creates the Unity Catalog structure needed for the ride analytics platform.
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

spark.sql(f"USE CATALOG {CATALOG}")
print(f"Catalog '{CATALOG}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema '{CATALOG}.{SCHEMA}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Create Volumes

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")
print(f"Volume '/Volumes/{CATALOG}/{SCHEMA}/checkpoints' ready")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")
print(f"Volume '/Volumes/{CATALOG}/{SCHEMA}/landing' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Verify

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
