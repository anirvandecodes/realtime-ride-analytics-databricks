# Real-Time Ride Analytics Platform on Databricks

End-to-end streaming analytics platform simulating how ride-booking companies monitor millions of rides in real time — built with Spark Structured Streaming, Lakeflow Declarative Pipelines, and Databricks AI/BI.

---

## Architecture

```
Ride Event Simulator
        │  writes .jsonl files · 15 events / 5s
        ▼
UC Volume  (/Volumes/workspace/realtime/landing)
        │  Auto Loader (cloudFiles)
        ▼
Bronze → Silver → Gold  (Delta Lake · Unity Catalog)
        │  Lakeflow Declarative Pipelines
        ▼
AI/BI Dashboard  (auto-refresh 30s)

Orchestration : Databricks Jobs
CI/CD         : Databricks Asset Bundles + GitHub Actions
```

---

## Project Structure

```
├── databricks.yml                          # bundle root
├── resources/
│   ├── pipeline.yml                        # Lakeflow pipeline definition
│   ├── dashboard.yml                       # AI/BI dashboard resource
│   └── executive_dashboard.lvdash.json    # dashboard layout & queries
└── src/
    ├── setup/
    │   └── 00_setup.py                     # one-time schema + volume setup
    ├── simulator/
    │   └── ride_simulator.py               # event generator → UC Volume
    ├── pipeline/
    │   └── transformations/
    │       ├── bronze.py                   # raw ingestion via Auto Loader
    │       ├── silver.py                   # cleansed + enriched events
    │       ├── gold_live_kpis.py           # real-time KPI aggregations
    │       ├── gold_revenue_per_minute.py
    │       ├── gold_rides_by_city.py
    │       ├── gold_top_zones.py
    │       ├── gold_ride_status_breakdown.py
    │       ├── gold_pickup_demand.py
    │       ├── gold_cancellation_analysis.py
    │       ├── gold_hourly_demand.py
    │       └── gold_driver_leaderboard.py
    └── demo/
        ├── streaming__manual_sss_to_declarative_pipelines.py  # migration demo
        └── batch_aggregations__merge_to_materialized_views.py
```

---

## Quickstart

**Prerequisites:** Databricks workspace · Databricks CLI

### 1. Deploy the bundle
```bash
databricks bundle deploy --target dev
```

### 2. Run setup (once)

Run `src/setup/00_setup.py` as a notebook — creates the `workspace.realtime` schema, landing volume, and checkpoints volume.

### 3. Start the simulator

Run the **Ride Simulator — Volume Writer** job from the Databricks Jobs UI, or trigger it via CLI:

```bash
databricks jobs run-now --job-id <simulator-job-id>
```

This writes 15 ride events every 5 seconds to the UC landing volume.

### 4. Start the pipeline

Run the **Ride Analytics Pipeline** from the Databricks Pipelines UI. It continuously streams Bronze → Silver → Gold using Auto Loader and Lakeflow Declarative Pipelines.

### 5. View the dashboard

Open **Ride Analytics — Executive Command Center** from the Databricks SQL Dashboards UI. It auto-refreshes every 30 seconds.

---

## Syncing Dashboard Changes

If you edit the dashboard directly in the Databricks UI, pull the changes back into the repo before the next deploy:

```bash
databricks bundle generate dashboard \
  --existing-id <dashboard-id> \
  --resource-dir resources \
  --dashboard-dir resources \
  --key executive_dashboard \
  --force
```

| Flag | Description | Default |
|------|-------------|---------|
| `--existing-id` | Dashboard ID to import | — |
| `--existing-path` | Workspace path (alternative to `--existing-id`) | — |
| `--resource-dir` | Where to write the `.dashboard.yml` | `resources/` |
| `--dashboard-dir` | Where to write the `.lvdash.json` | `src/` |
| `--key` | Resource key in the generated YAML | — |
| `--force` | Overwrite existing files | false |
| `--watch` | Continuously sync UI changes to local files | false |

Works for **any** dashboard in your workspace — even ones not yet tracked in a bundle. Find the dashboard ID in the URL:
`https://<workspace>.cloud.databricks.com/sql/dashboardsv3/<dashboard-id>`

---

## Simulated Data

- **Cities:** Bangalore · Mumbai · Delhi · Chennai · Hyderabad
- **Vehicle types:** Bike · Auto · Mini · Sedan · SUV
- **Ride status mix:** 72% completed · 13% user-cancelled · 8% driver-cancelled · 7% no driver
- **Payment mix:** UPI 52% · Cash 22% · Card 15% · Wallet 11%
- **Peak hours:** 8–10am and 6–9pm with 1.3–2.0× surge multiplier
