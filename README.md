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

### 2. Deploy
```bash
databricks bundle deploy --target dev \
  --var="kafka_bootstrap_servers=<your-bootstrap>:9092"
```

### 3. Run setup notebook (once)
`src/setup/00_setup.py` — creates the `workspace.realtime` schema and checkpoints volume.

### 4. Start the simulator
`src/simulator/ride_simulator.py` — streams ride events to Kafka indefinitely.

### 5. Start the pipeline
`src/pipeline/ride_pipeline.py` — run as a Databricks Job. Streams Bronze → Silver → Gold.

### 6. Open the dashboard
Connect Databricks AI/BI to the Gold tables. Set auto-refresh to 30 seconds.

---

## Data

- 5 cities: Bangalore, Mumbai, Delhi, Chennai, Hyderabad
- 5 vehicle types: Bike, Auto, Mini, Sedan, SUV
- Status mix: 72% completed · 13% user-cancelled · 8% driver-cancelled · 7% no driver
- Payment: UPI 52% · Cash 22% · Card 15% · Wallet 11%
- Peak hours: 8–10am and 6–9pm with 1.3–2.0× surge
