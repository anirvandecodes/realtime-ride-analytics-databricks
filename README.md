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

If you've edited the dashboard directly in the Databricks UI, pull the latest version back into the repo before deploying:

```bash
databricks bundle generate dashboard --existing-id <dashboard-id> \
  --target dev \
  > resources/executive_dashboard.lvdash.json
```

Or use the Databricks CLI to download it directly:

```bash
databricks api get /api/2.0/lakeview/dashboards/<dashboard-id> \
  | python3 -c "import sys, json; d=json.load(sys.stdin); print(d['serialized_dashboard'])" \
  | python3 -c "import sys, json; print(json.dumps(json.loads(sys.stdin.read()), indent=2))" \
  > resources/executive_dashboard.lvdash.json
```

Find your dashboard ID in the URL when viewing it in the workspace:
`https://<workspace>.azuredatabricks.net/sql/dashboardsv3/<dashboard-id>`

### 8. Deploy the dashboard

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
