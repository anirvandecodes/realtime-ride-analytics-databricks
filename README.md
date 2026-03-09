# Real-Time Ride Analytics Platform on Databricks

End-to-end streaming platform built with Confluent Kafka, Spark Structured Streaming, and Databricks AI/BI — simulating how ride-booking companies monitor millions of rides in real time.

---

## Architecture

```
Ride Event Simulator
        │  confluent-kafka producer · 15 events / 5s
        ▼
Confluent Kafka  (ride-events topic)
        │  Spark Structured Streaming
        ▼
Bronze → Silver → Gold  (Delta Lake · Unity Catalog)
        │  foreachBatch · 30s refresh
        ▼
AI/BI Dashboard  (3 pages · auto-refresh 30s)

Orchestration : Databricks Jobs
CI/CD         : Databricks Asset Bundles + GitHub Actions
```

---

## Project Structure

```
├── databricks.yml
├── resources/
│   ├── pipeline.yml
│   └── job.yml
└── src/
    ├── setup/ride_simulator.py
    ├── simulator/ride_simulator.py
    └── pipeline/ride_pipeline.py
```

---

## Quickstart

**Prerequisites:** Databricks workspace · Databricks CLI · Confluent Cloud account

### 1. Set up secrets
```bash
databricks secrets create-scope confluent-kafka
databricks secrets put-secret confluent-kafka api-key    --string-value "<KEY>"
databricks secrets put-secret confluent-kafka api-secret --string-value "<SECRET>"
```

### 2. Allow Kafka library (admin required)

Add the Spark Kafka connector to the workspace artifact allowlist:

```bash
databricks artifact-allowlists update LIBRARY_MAVEN --json '{
  "artifact_matchers": [
    {"artifact": "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0", "match_type": "PREFIX_MATCH"}
  ]
}'
```

Or via the UI: **Workspace Settings → Compute → Libraries → Allowlist**

### 3. Deploy
```bash
databricks bundle deploy --target dev \
  --var="kafka_bootstrap_servers=<your-bootstrap>:9092" \
  --var="cluster_id=<existing-cluster-id>"
```

### 4. Run setup notebook (once)
`src/setup/00_setup.py` — creates the `workspace.realtime` schema and checkpoints volume.

### 5. Start the simulator
`src/simulator/ride_simulator.py` — streams ride events to Kafka indefinitely.

### 6. Start the pipeline
`src/pipeline/ride_pipeline.py` — run as a Databricks Job. Streams Bronze → Silver → Gold.

### 7. Deploy the dashboard

Upload `Ride Operations Command Center.lvdash.json` to your workspace and publish it:

```bash
# Create the dashboard
databricks api post /api/2.0/lakeview/dashboards --json "{
  \"display_name\": \"Ride Operations Command Center\",
  \"parent_path\": \"/Workspace/Users/<your-email>\",
  \"serialized_dashboard\": $(python3 -c "import json; print(json.dumps(open('Ride Operations Command Center.lvdash.json').read()))")
}"

# Publish it (replace <dashboard-id> with the id returned above)
databricks api post /api/2.0/lakeview/dashboards/<dashboard-id>/published \
  --json '{"warehouse_id": "<your-warehouse-id>"}'
```

The dashboard has 3 pages:
- **Live Command Center** — real-time KPIs, revenue pulse, ride status breakdown
- **City & Zone Intelligence** — city comparison, top revenue zones
- **Demand & Pricing Strategy** — hourly demand, surge multiplier, supply-demand gap

---

## Data

- 5 cities: Bangalore, Mumbai, Delhi, Chennai, Hyderabad
- 5 vehicle types: Bike, Auto, Mini, Sedan, SUV
- Status mix: 72% completed · 13% user-cancelled · 8% driver-cancelled · 7% no driver
- Payment: UPI 52% · Cash 22% · Card 15% · Wallet 11%
- Peak hours: 8–10am and 6–9pm with 1.3–2.0× surge
