# 🚕 NYC Yellow Taxi — Real-time Data Pipeline

> An end-to-end streaming data pipeline built on Databricks, processing **119M+ records (5GB+)** of NYC Yellow Taxi trip data using a modern Lakehouse architecture.

---

## 🏗️ Architecture

```
NYC TLC Parquet Files (5GB+)
          │
          ▼
┌─────────────────────┐
│     Auto Loader      │  Spark Structured Streaming
│   (cloudFiles)       │  detects new files automatically
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│       BRONZE        │  Raw ingestion + metadata columns
│  yellow_taxi        │  (ingested_at, source_file, ingest_date)
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│      SILVER 1       │  Cleaned & deduplicated
│  yellow_taxi_clean  │  Outlier filtering + null handling
│                     │  Delta MERGE INTO (deduplication)
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│      SILVER 2       │  Enriched with geospatial data
│ yellow_taxi_enriched│  NYC Zone Lookup join
│                     │  + is_airport_trip, is_rush_hour
│                     │  + tip_percentage, payment_type_desc
└────────┬────────────┘
         │
   ┌─────┴──────┬──────────────┐
   ▼            ▼              ▼
┌──────┐   ┌────────┐   ┌──────────┐
│GOLD 1│   │ GOLD 2 │   │  GOLD 3  │
│Revenue   │  Trip  │   │  Vendor  │
│Dashboard │Analysis│   │Performance
└──────┘   └────────┘   └──────────┘
Tumbling   Tumbling      Batch
Window     Window        aggregation
+ Watermark+ Watermark
```

---

## ✨ Key Features

- **Spark Structured Streaming** with `availableNow` trigger for incremental processing
- **Databricks Auto Loader** (`cloudFiles`) for automatic file detection
- **Extended Medallion Architecture** — Bronze → Silver 1 → Silver 2 → Gold 1/2/3
- **Unity Catalog** for data governance and lineage tracking
- **Delta Lake MERGE INTO** for deduplication at Silver layer
- **Tumbling Window + Watermark** at Gold layer for late-arriving data handling
- **Databricks Workflows** with parameterized jobs for daily scheduled runs
- **Centralized config** (`00_config`) for environment switching (dev/prod)

---

## 📊 Dataset

| Property | Value |
|---|---|
| Source | [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| Format | Parquet |
| Size | ~2 GB |
| Records | 128M+ rows |
| Period | 2023 – 2025 |

---

## 🛠️ Tech Stack

| Category | Tools |
|---|---|
| Platform | Databricks (Free Edition) |
| Language | PySpark |
| Storage | Delta Lake, Unity Catalog |
| Ingestion | Databricks Auto Loader |
| Streaming | Spark Structured Streaming |
| Orchestration | Databricks Workflows |
| Architecture | Medallion Architecture |

---

## 📁 Project Structure

```
nyc-taxi-databricks-pipeline/
├── 00_config                         # Centralized config & parameters
├── Task 1 - Setup Unity Catalog      # Catalog, Schema, Volume setup
├── Task 2 - Bronze                   # Auto Loader → Bronze Delta Table
├── Task 3 - Silver: Cleaned          # Cleaning, dedup → Silver 1
├── Task 4 - Silver: Enriched         # Zone join, feature eng → Silver 2
├── Task 5 - Gold: Revenue Dashboard  # Revenue by tumbling window
├── Task 6 - Gold: Trip Analysis      # Trip patterns by zone & time
└── Task 7 - Gold: Vendor Performance # Vendor comparison (batch)
```

---

## 🔄 Pipeline Layers

### Bronze — Raw Ingestion
- Ingests raw Parquet files via Auto Loader
- Adds metadata: `ingested_at`, `source_file`, `ingest_date`
- Recovers mismatched columns from `_rescued_data`
- Schema evolution with `addNewColumns` mode

### Silver 1 — Cleaned
- Filters outliers: `fare_amount > 0`, `trip_distance ≤ 200`, `passenger_count ≤ 6`
- Handles nulls via `COALESCE` from `_rescued_data`
- Deduplicates using Delta **MERGE INTO** on `(VendorID, pickup_datetime, dropoff_datetime)`
- Derives: `pickup_hour`, `pickup_day`, `trip_duration_minutes`

### Silver 2 — Enriched
- Joins **NYC Taxi Zone Lookup** to map location IDs → borough/zone names
- Adds business flags: `is_airport_trip`, `is_rush_hour`
- Computes `tip_percentage`, maps `payment_type` to descriptions

### Gold 1 — Revenue Dashboard
- **1-hour Tumbling Window** + **1-day Watermark**
- Aggregates: `total_revenue`, `avg_fare`, `avg_tip_pct`, `airport_trips`, `rush_hour_trips`
- Grouped by: `window`, `pickup_borough`, `payment_type_desc`

### Gold 2 — Trip Analysis
- **1-hour Tumbling Window** + **1-day Watermark**
- Top routes by `pickup_zone → dropoff_zone`
- Insights: avg duration, avg fare, airport trip counts per hour

### Gold 3 — Vendor Performance
- Batch aggregation by `vendor_name`, `pickup_borough`, `year/month`
- Compares **Creative Mobile Technologies** vs **VeriFone Inc**
- Metrics: revenue, tip rate, avg distance, rush hour trips

---

## ⚙️ Workflow Orchestration

Pipeline is automated via **Databricks Workflows** with the following DAG:

```
Land_New_Data → Bronze → Silver1 → Silver2 → Gold1 (Revenue)
                                            → Gold2 (Trip Analysis)
                                            → Gold3 (Vendor Perf)
```

Gold 1, 2, 3 run **in parallel** after Silver 2 completes.

Scheduled: **Daily at 6:00 AM UTC**

---
