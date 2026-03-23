# Real-Time Ride Analytics Platform on Databricks

End-to-end streaming platform built with Confluent Kafka, Spark Structured Streaming, and Databricks AI/BI — simulating how ride-booking companies monitor millions of rides in real time.

---

## Architecture

```
Ride Event Simulator
        │  writes .jsonl files · 15 events / 5s
        ▼
UC Volume  (/Volumes/workspace/realtime/landing)
        │  Auto Loader (cloudFiles) · Spark Structured Streaming
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

**Prerequisites:** Databricks workspace · Databricks CLI

### 1. Deploy
```bash
databricks bundle deploy --target dev
```

### 2. Run setup notebook (once)  *(was step 4)*

`src/setup/00_setup.py` — creates the `workspace.realtime` schema, checkpoints volume, and landing volume.

### 5. Start the simulator
`src/simulator/ride_simulator.py` — streams ride events to Kafka indefinitely.

### 6. Start the pipeline
`src/pipeline/ride_pipeline.py` — run as a Databricks Job. Streams Bronze → Silver → Gold.

### 7. Pull the latest dashboard definition (if modified remotely)

If you've edited the dashboard directly in the Databricks UI, pull the latest version back into the repo:

```bash
databricks bundle generate dashboard --existing-id <dashboard-id> --target dev
```

This generates both the `.lvdash.json` file and the corresponding `dashboard.yml` resource entry. It works for **any** dashboard in your workspace — even ones not yet tracked in a bundle.

Find the dashboard ID in the URL when viewing it:
`https://<workspace>.cloud.databricks.com/sql/dashboardsv3/<dashboard-id>`

---

## Data

- 5 cities: Bangalore, Mumbai, Delhi, Chennai, Hyderabad
- 5 vehicle types: Bike, Auto, Mini, Sedan, SUV
- Status mix: 72% completed · 13% user-cancelled · 8% driver-cancelled · 7% no driver
- Payment: UPI 52% · Cash 22% · Card 15% · Wallet 11%
- Peak hours: 8–10am and 6–9pm with 1.3–2.0× surge
