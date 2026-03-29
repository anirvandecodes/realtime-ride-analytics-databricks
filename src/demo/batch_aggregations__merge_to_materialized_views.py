# Databricks notebook source
# MAGIC %md
# MAGIC # Materialized Views — Incremental Refresh Demo
# MAGIC
# MAGIC **This notebook shows two ways to keep an aggregation table fresh when new data arrives:**
# MAGIC 1. **The manual way** — write a Delta table + MERGE it yourself every time
# MAGIC 2. **The Materialized View way** — declare the query once, let Databricks handle the rest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Create source table and load seed data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.demo;
# MAGIC
# MAGIC DROP TABLE IF EXISTS workspace.demo.orders;
# MAGIC
# MAGIC CREATE TABLE workspace.demo.orders (
# MAGIC   order_id  STRING,
# MAGIC   customer  STRING,
# MAGIC   category  STRING,
# MAGIC   amount    INT,
# MAGIC   status    STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.demo.orders VALUES
# MAGIC   ('ORD-001', 'Alice', 'Electronics', 1200, 'delivered'),
# MAGIC   ('ORD-002', 'Bob',   'Clothing',     350, 'delivered'),
# MAGIC   ('ORD-003', 'Alice', 'Books',          89, 'delivered'),
# MAGIC   ('ORD-004', 'Carol', 'Electronics',   780, 'delivered'),
# MAGIC   ('ORD-005', 'Bob',   'Food',           45, 'cancelled'),
# MAGIC   ('ORD-006', 'Carol', 'Clothing',      210, 'delivered');

# COMMAND ----------

# MAGIC %md
# MAGIC 6 orders across 4 categories. ORD-005 is **cancelled** — we only want delivered orders in our aggregation.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.demo.orders ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1 — The Manual Way (MERGE)
# MAGIC
# MAGIC Goal: keep a pre-aggregated table — revenue by category — fresh as new orders arrive.
# MAGIC
# MAGIC Without a Materialized View, you'd create a Delta table and manually MERGE new data into it.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1A — Create the aggregation table and seed it

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.demo.manual_revenue_by_category (
# MAGIC   category        STRING,
# MAGIC   total_orders    BIGINT,
# MAGIC   total_revenue   BIGINT,
# MAGIC   avg_order_value DOUBLE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO workspace.demo.manual_revenue_by_category
# MAGIC SELECT
# MAGIC   category,
# MAGIC   COUNT(*)              AS total_orders,
# MAGIC   SUM(amount)           AS total_revenue,
# MAGIC   ROUND(AVG(amount), 2) AS avg_order_value
# MAGIC FROM workspace.demo.orders
# MAGIC WHERE status = 'delivered'
# MAGIC GROUP BY category;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.demo.manual_revenue_by_category ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1B — New orders arrive

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.demo.orders VALUES
# MAGIC   ('ORD-007', 'Dave',  'Electronics', 2500, 'delivered'),
# MAGIC   ('ORD-008', 'Alice', 'Clothing',     620, 'delivered'),
# MAGIC   ('ORD-009', 'Dave',  'Books',        150, 'delivered');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1C — Manually MERGE to refresh the aggregation
# MAGIC
# MAGIC You must write this MERGE, run it, and schedule it yourself — every time new data lands.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO workspace.demo.manual_revenue_by_category AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     category,
# MAGIC     COUNT(*)              AS total_orders,
# MAGIC     SUM(amount)           AS total_revenue,
# MAGIC     ROUND(AVG(amount), 2) AS avg_order_value
# MAGIC   FROM workspace.demo.orders
# MAGIC   WHERE status = 'delivered'
# MAGIC   GROUP BY category
# MAGIC ) AS source
# MAGIC ON target.category = source.category
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.total_orders    = source.total_orders,
# MAGIC   target.total_revenue   = source.total_revenue,
# MAGIC   target.avg_order_value = source.avg_order_value
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Numbers updated -- but only because YOU ran the MERGE
# MAGIC SELECT * FROM workspace.demo.manual_revenue_by_category ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ It works. But **you own everything**: writing the MERGE, scheduling it, handling errors, one MERGE per aggregation table.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2 — The Materialized View Way
# MAGIC
# MAGIC Same goal. Declare the query once — Databricks handles the refresh.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2A — Create the Materialized View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW workspace.demo.mv_revenue_by_category AS
# MAGIC SELECT
# MAGIC   category,
# MAGIC   COUNT(*)              AS total_orders,
# MAGIC   SUM(amount)           AS total_revenue,
# MAGIC   ROUND(AVG(amount), 2) AS avg_order_value
# MAGIC FROM workspace.demo.orders
# MAGIC WHERE status = 'delivered'
# MAGIC GROUP BY category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2B — Query it — result is pre-computed and stored

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.demo.mv_revenue_by_category;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2C — Add more orders (same 3 rows as before)
# MAGIC
# MAGIC We are NOT touching the MV definition.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.demo.orders VALUES
# MAGIC   ('ORD-010', 'Eve',   'Electronics', 1800, 'delivered'),
# MAGIC   ('ORD-011', 'Frank', 'Clothing',     490, 'delivered'),
# MAGIC   ('ORD-012', 'Eve',   'Books',         75, 'delivered');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2D — Refresh and query

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH MATERIALIZED VIEW workspace.demo.mv_revenue_by_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Same query — numbers updated automatically
# MAGIC SELECT * FROM workspace.demo.mv_revenue_by_category;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Manual MERGE vs Materialized View
# MAGIC
# MAGIC | | Manual MERGE | Materialized View |
# MAGIC |---|---|---|
# MAGIC | Who writes the refresh logic | **You** | Databricks |
# MAGIC | Who schedules it | **You** (job/cron) | Databricks |
# MAGIC | Handles new categories | ✅ WHEN NOT MATCHED | ✅ Automatic |
# MAGIC | Handles deletes in source | ❌ Extra logic needed | ✅ Automatic |
# MAGIC | One refresh for all tables | ❌ One MERGE per table | ✅ One REFRESH |
# MAGIC | Lineage tracking | ❌ None | ✅ Built-in |
# MAGIC
# MAGIC **MERGE works** — but you own every piece of it.
# MAGIC With a Materialized View, you write the query once and Databricks owns the rest.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3 — How the Engine Chooses the Refresh Strategy
# MAGIC
# MAGIC When you `REFRESH MATERIALIZED VIEW`, Databricks doesn't blindly recompute everything.
# MAGIC The engine inspects the query and the source data to pick the most efficient strategy automatically.
# MAGIC
# MAGIC | Strategy | When the engine picks it | What it does |
# MAGIC |---|---|---|
# MAGIC | **Full recompute** | Non-incremental queries (e.g. global aggregations with no partition key) | Recomputes the entire MV from scratch |
# MAGIC | **Partition overwrite** | MV is partitioned and only some source partitions changed | Rewrites only the affected partitions |
# MAGIC | **Incremental aggregation** | Append-only source + aggregation with a grouping key | Merges new aggregate deltas into existing results |
# MAGIC | **Skip (no-op)** | Source data unchanged since last refresh | Returns immediately — nothing to do |
# MAGIC
# MAGIC You never write this logic. You declare the query; the engine picks the strategy at runtime based on Delta Lake change tracking.
