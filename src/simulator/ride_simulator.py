# Databricks notebook source
# MAGIC %md
# MAGIC # Ride Event Simulator
# MAGIC Generates realistic Indian ride-booking events and writes them as JSON files
# MAGIC to a **Unity Catalog Volume** every 5 seconds — simulating a live production feed.
# MAGIC
# MAGIC **Keep this notebook running** while the Lakeflow pipeline is active.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

LANDING_PATH  = "/Volumes/workspace/realtime/landing"
BATCH_SIZE    = 15
INTERVAL_SECS = 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## City & Area Data

# COMMAND ----------

import random
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

CITY_DATA = {
    "Bangalore": {
        "code": "BLR",
        "zones": ["BLR-Z1", "BLR-Z2", "BLR-Z3", "BLR-Z4", "BLR-Z5"],
        "areas": ["Koramangala", "Whitefield", "Indiranagar", "HSR Layout",
                  "Electronic City", "MG Road", "Marathahalli", "Jayanagar",
                  "Bannerghatta Road", "Yelahanka"],
    },
    "Mumbai": {
        "code": "MUM",
        "zones": ["MUM-Z1", "MUM-Z2", "MUM-Z3", "MUM-Z4", "MUM-Z5"],
        "areas": ["Bandra", "Andheri", "Powai", "Lower Parel", "Juhu",
                  "Dadar", "Kurla", "Borivali", "Thane", "Navi Mumbai"],
    },
    "Delhi": {
        "code": "DEL",
        "zones": ["DEL-Z1", "DEL-Z2", "DEL-Z3", "DEL-Z4", "DEL-Z5"],
        "areas": ["Connaught Place", "Dwarka", "Noida Sector 18", "Gurgaon Cyber City",
                  "Lajpat Nagar", "Saket", "Rohini", "Karol Bagh", "Vasant Kunj", "Nehru Place"],
    },
    "Chennai": {
        "code": "CHN",
        "zones": ["CHN-Z1", "CHN-Z2", "CHN-Z3", "CHN-Z4", "CHN-Z5"],
        "areas": ["T Nagar", "Anna Nagar", "OMR", "Velachery", "Adyar",
                  "Guindy", "Porur", "Tambaram", "Perambur", "Mylapore"],
    },
    "Hyderabad": {
        "code": "HYD",
        "zones": ["HYD-Z1", "HYD-Z2", "HYD-Z3", "HYD-Z4", "HYD-Z5"],
        "areas": ["Hitech City", "Banjara Hills", "Gachibowli", "Madhapur",
                  "Jubilee Hills", "Kondapur", "Kukatpally", "Secunderabad",
                  "Ameerpet", "Miyapur"],
    },
}

VEHICLE_CONFIG = {
    "Bike":  {"base": 15, "per_km": 8,  "min": 30,  "max": 150},
    "Auto":  {"base": 25, "per_km": 12, "min": 50,  "max": 200},
    "Mini":  {"base": 40, "per_km": 14, "min": 100, "max": 400},
    "Sedan": {"base": 60, "per_km": 18, "min": 150, "max": 600},
    "SUV":   {"base": 80, "per_km": 22, "min": 250, "max": 800},
}

STATUS_WEIGHTS = {
    "completed":           0.72,
    "cancelled_by_user":   0.13,
    "cancelled_by_driver": 0.08,
    "no_driver_found":     0.07,
}

CANCELLATION_REASONS = {
    "cancelled_by_user":   ["Changed my mind", "Ride taking too long", "Found alternate transport",
                            "Booked by mistake", "Driver too far"],
    "cancelled_by_driver": ["Personal emergency", "Vehicle breakdown", "Wrong route assigned",
                            "Unable to reach pickup", "App issue"],
    "no_driver_found":     ["No drivers available in area", "High demand surge",
                            "Late night low availability"],
}

PAYMENT_METHODS = ["UPI", "Cash", "Card", "Wallet"]
PAYMENT_WEIGHTS = [0.52, 0.22, 0.15, 0.11]

DRIVER_FIRST = ["Rajesh", "Suresh", "Ramesh", "Mahesh", "Dinesh", "Ganesh", "Naresh",
                "Pradeep", "Santosh", "Vikram", "Ajay", "Ravi", "Arun", "Kiran", "Mohan",
                "Sanjay", "Deepak", "Anand", "Venkat", "Prasad"]
DRIVER_LAST  = ["Kumar", "Singh", "Sharma", "Yadav", "Gupta", "Patel", "Reddy",
                "Nair", "Pillai", "Rao", "Verma", "Joshi", "Chauhan", "Mishra", "Tiwari"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def is_peak_hour(dt: datetime) -> bool:
    h = dt.hour
    return (8 <= h < 10) or (18 <= h < 21)

def get_surge(is_peak: bool) -> float:
    if is_peak:
        return round(random.choice([1.3, 1.5, 1.8, 2.0]), 1)
    return 1.0

def calc_fare(vehicle: str, distance_km: float, surge: float) -> dict:
    cfg  = VEHICLE_CONFIG[vehicle]
    base = cfg["base"] + cfg["per_km"] * distance_km
    fare = round(min(max(base * surge, cfg["min"]), cfg["max"]), 2)
    return {"base_fare": round(base, 2), "final_fare": fare}

def generate_ride_event() -> dict:
    now     = datetime.now(IST)
    peak    = is_peak_hour(now)
    city    = random.choice(list(CITY_DATA.keys()))
    cdata   = CITY_DATA[city]
    areas   = cdata["areas"]
    pickup  = random.choice(areas)
    drop    = random.choice([a for a in areas if a != pickup])
    vehicle = random.choices(
        list(VEHICLE_CONFIG.keys()),
        weights=[0.15, 0.20, 0.30, 0.25, 0.10]
    )[0]
    distance = round(random.uniform(1.0, 22.0), 1)
    duration = int(distance * random.uniform(2.5, 4.5))
    surge    = get_surge(peak)
    fares    = calc_fare(vehicle, distance, surge)
    status   = random.choices(
        list(STATUS_WEIGHTS.keys()),
        weights=list(STATUS_WEIGHTS.values())
    )[0]
    cancel_reason = (
        random.choice(CANCELLATION_REASONS[status])
        if status in CANCELLATION_REASONS else None
    )
    driver_id   = f"DRV-{cdata['code']}-{random.randint(1000, 9999)}"
    driver_name = f"{random.choice(DRIVER_FIRST)} {random.choice(DRIVER_LAST)}"
    ride_id     = f"RD-{now.strftime('%Y%m%d')}-{cdata['code']}-{random.randint(10000, 99999)}"

    return {
        "ride_id":             ride_id,
        "event_time":          now.isoformat(),
        "status":              status,
        "city":                city,
        "pickup_area":         pickup,
        "drop_area":           drop,
        "vehicle_type":        vehicle,
        "driver_id":           driver_id,
        "driver_name":         driver_name,
        "driver_rating":       round(random.uniform(3.5, 5.0), 1),
        "rider_id":            f"USR-{random.randint(10000, 99999)}",
        "distance_km":         distance,
        "duration_mins":       duration,
        "base_fare":           fares["base_fare"],
        "surge_multiplier":    surge,
        "final_fare":          fares["final_fare"],
        "payment_method":      random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
        "cancellation_reason": cancel_reason,
        "zone_id":             random.choice(cdata["zones"]),
        "is_peak_hour":        peak,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Simulator
# MAGIC Writes one batch of ride events as a JSON file to the landing volume every 5 seconds.
# MAGIC Each file = 15 ride events (one JSON object per line).

# COMMAND ----------

dbutils.fs.mkdirs(LANDING_PATH)

print(f"Starting ride simulator → Volume '{LANDING_PATH}'")
print(f"Batch size: {BATCH_SIZE} rides | Interval: {INTERVAL_SECS}s\n")

batch_num = 0
while True:
    batch_num += 1
    now    = datetime.now(IST)
    events = [generate_ride_event() for _ in range(BATCH_SIZE)]

    file_path = f"{LANDING_PATH}/rides_{now.strftime('%Y%m%d_%H%M%S')}_{batch_num:05d}.jsonl"
    jsonl     = "\n".join(json.dumps(e) for e in events)
    dbutils.fs.put(file_path, jsonl, overwrite=True)

    completed = sum(1 for e in events if e["status"] == "completed")
    revenue   = sum(e["final_fare"] for e in events if e["status"] == "completed")
    print(f"[Batch {batch_num:05d}] {len(events)} events written | "
          f"{completed} completed | ₹{revenue:,.0f} revenue | {file_path.split('/')[-1]}")

    time.sleep(INTERVAL_SECS)
