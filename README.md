# Amusement Park Simulation — Real-Time Data Pipeline

A end-to-end data engineering project simulating a theme park's operational data platform. Built as a portfolio project demonstrating real-time event streaming, cloud data warehousing, and orchestrated transformations.

---

## Architecture overview

```
Simulation APIs (FastAPI)
        ↓
Kafka Producers
        ↓
Kafka Topics (3)
        ↓
S3 Consumer → AWS S3 (Bronze)
        ↓
Airflow (hourly)
        ↓
Snowflake RAW → CORE (silver) → GOLD (fact/dim)
        ↓
Grafana Dashboard
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Simulation | FastAPI, Python |
| Streaming | Apache Kafka (Confluent) |
| Object storage | AWS S3 |
| Orchestration | Apache Airflow |
| Data warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Dashboarding | Grafana |
| Infrastructure | Docker Compose |

---

## Project structure

```
fantasyland/
├── apis/
│   ├── arrivals_api.py          # hotel checkins, park entries, queue entries
│   ├── transactions_api.py      # ticket sales, food sales, gift store sales
│   └── performances_api.py      # shows, parades, character meets (static schedule)
│
├── producers/
│   ├── producer.py              # polls arrivals API → fantasyland-arrivals topic
│   ├── transactions_producer.py # polls transactions API → fantasyland-transactions topic
│   └── performances_producer.py # polls performances API → fantasyland-performances topic
│
├── consumers/
│   └── s3_consumer.py           # reads all topics → writes JSON files to S3
│
├── db/
│   ├── snowflake_connection.py  # shared Snowflake connection
│   ├── snowflake_guests.py      # loads guest pool from Snowflake at startup
│   ├── guests.py                # local fallback guest pool (200 guests)
│   └── migrations/
│       ├── create_raw_tables.py         # creates AMUSEMENTPARK.RAW schema + tables
│       ├── create_gold_tables.py        # creates AMUSEMENTPARK.GOLD dims + facts
│       ├── load_raw_from_s3.py          # COPY INTO from S3 → RAW (called by Airflow)
│       └── create_performance_tables.sql
│
├── seed/
│   ├── seed_guests.py           # loads 10,000 guests into CORE.GUESTS via CSV
│   └── export_performances.py   # exports 2026 performance schedule to CSVs
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml             # Snowflake connection for dbt
│   ├── macros/
│   │   └── generate_schema_name.sql  # overrides dbt schema naming
│   └── models/
│       ├── silver/              # RAW → CORE staging models
│       │   ├── sources.yml
│       │   ├── schema.yml
│       │   ├── stg_park_entry.sql
│       │   ├── stg_hotel_checkin.sql
│       │   ├── stg_queue_entry.sql
│       │   ├── stg_ticket_sales.sql
│       │   ├── stg_food_sales.sql
│       │   └── stg_gift_store_sales.sql
│       └── gold/                # CORE → GOLD fact + dim models
│           ├── schema.yml
│           ├── fact_park_entry.sql
│           ├── fact_queue_entry.sql
│           ├── fact_ticket_sale.sql
│           ├── fact_food_sale.sql
│           └── fact_gift_store_sale.sql
│
├── dags/
│   ├── fantasyland_arrivals_load.py      # hourly S3 → RAW load (arrivals)
│   ├── fantasyland_transactions_load.py  # hourly S3 → RAW load (transactions)
│   ├── fantasyland_dbt_silver.py         # hourly dbt silver run
│   ├── fantasyland_dbt_gold.py           # hourly dbt gold run
│   └── fantasyland_guests_refresh.py     # daily guest seed refresh
│
├── databricks/
│   └── load_raw_from_s3.py      # Databricks notebook version of the S3 loader
│
├── grafana/
│   └── dashboard_queries.sql    # SQL for all Grafana panels
│
├── docker-compose.yml           # Kafka, Zookeeper, Airflow, Grafana
├── airflow-requirements.txt     # extra pip packages for Airflow container
├── requirements.txt             # Python dependencies for local scripts
├── .env.example                 # environment variable template
└── .gitignore
```

---

## Snowflake schema layout

```
AMUSEMENTPARK
├── RAW          Bronze layer — raw JSON events loaded from S3
│   ├── RAW_PARK_ENTRY
│   ├── RAW_HOTEL_CHECKIN
│   ├── RAW_QUEUE_ENTRY
│   ├── RAW_TICKET_SALE_ONLINE
│   ├── RAW_TICKET_SALE_GATE
│   ├── RAW_FOOD_SALE
│   └── RAW_GIFT_STORE_SALE
│
├── CORE         Silver layer — cleaned, deduplicated, typed
│   ├── GUESTS                   (master data — 10,000 guests)
│   ├── PERFORMANCES             (static 2026 show schedule)
│   ├── PERFORMANCE_CAST
│   ├── PERFORMANCE_CHARACTERS
│   ├── STG_PARK_ENTRY
│   ├── STG_HOTEL_CHECKIN
│   ├── STG_QUEUE_ENTRY
│   ├── STG_TICKET_SALES
│   ├── STG_FOOD_SALES
│   └── STG_GIFT_STORE_SALES
│
└── GOLD         Gold layer — fact tables, dimensions, event audit
    ├── EVENT_HEADER             (audit trail — event_id, loaded_at, s3 file)
    ├── DIM_PARK
    ├── DIM_RIDE
    ├── DIM_TICKET_TYPE
    ├── DIM_DATE                 (all of 2026 pre-populated)
    ├── FACT_PARK_ENTRY
    ├── FACT_QUEUE_ENTRY
    ├── FACT_TICKET_SALE
    ├── FACT_FOOD_SALE
    └── FACT_GIFT_STORE_SALE
```

---

## Setup

### Prerequisites

- Python 3.11+
- Docker Desktop
- AWS account with S3 bucket
- Snowflake account (free trial works)

### 1. Clone and install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# fill in your Snowflake, AWS, and Kafka values
```

### 3. Start infrastructure

```bash
docker-compose up -d
```

Wait ~2 minutes for Kafka topics to be created and Airflow to initialize.

### 4. Set up Snowflake

```bash
# Create RAW schema and tables
python db/migrations/create_raw_tables.py

# Create GOLD schema, dims, and fact tables
python db/migrations/create_gold_tables.py
```

### 5. Seed reference data

```bash
# Load 10,000 guests into CORE.GUESTS
python seed/seed_guests.py

# Export 2026 performance schedule to CSVs
python seed/export_performances.py
# then load CSVs via create_performance_tables.sql in Snowflake UI
```

### 6. Start the APIs

```bash
# Three separate terminals
python arrivals_api.py          # port 8000
python transactions_api.py      # port 8001
python performances_api.py      # port 8002
```

### 7. Start the producers

```bash
# Three separate terminals
python producer.py
python transactions_producer.py
python performances_producer.py
```

### 8. Start the S3 consumer

```bash
python s3_consumer.py
```

### 9. Enable Airflow DAGs

Open `http://localhost:8080` (admin / admin) and enable:

| DAG | Schedule | What it does |
|---|---|---|
| `fantasyland_arrivals_load` | hourly :05 | S3 → Snowflake RAW (arrivals) |
| `fantasyland_transactions_load` | hourly :05 | S3 → Snowflake RAW (transactions) |
| `fantasyland_dbt_silver` | hourly :15 | RAW → CORE silver models |
| `fantasyland_dbt_gold` | hourly :30 | CORE → GOLD fact tables |
| `fantasyland_guests_refresh` | daily 2am | refresh guest pool |

### 10. Open Grafana

Go to `http://localhost:3000` (admin / admin), install the Snowflake plugin, add a Snowflake data source, and use the queries in `grafana/dashboard_queries.sql` to build panels.

---

## Kafka topics

| Topic | Producer | Consumer | Contents |
|---|---|---|---|
| `fantasyland-arrivals` | `producer.py` | `s3_consumer.py` | park entries, hotel checkins, queue entries |
| `fantasyland-transactions` | `transactions_producer.py` | `s3_consumer.py` | ticket sales, food sales, gift sales |
| `fantasyland-performances` | `performances_producer.py` | `s3_consumer.py` | shows, parades, character meets |

All topics have 3 partitions. Events are keyed by `guest_id` so all activity for the same guest lands in the same partition in chronological order.

---

## S3 file structure

Events are written as individual JSON files using Hive-style time partitioning:

```
your-bucket/
  arrivals/
    year=2026/month=03/day=29/hour=14/
      PARK_ENTRY_<event_id>.json
      HOTEL_CHECKIN_<event_id>.json
  transactions/
    year=2026/month=03/day=29/hour=14/
      TICKET_SALE_ONLINE_<event_id>.json
      FOOD_SALE_<event_id>.json
```

Snowflake's `COPY INTO` uses load history to skip already-processed files, so re-running an hourly load is always safe.

---

## Airflow pipeline flow

```
:05 — load_arrivals    ─┐
:05 — load_transactions ┘ (parallel, S3 → Snowflake RAW)
:15 — dbt_silver          (RAW → CORE, incremental)
:30 — dbt_gold            (CORE → GOLD, incremental)
2am — guests_refresh      (daily, regenerates guest pool)
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Arrivals API | http://localhost:8000/docs | — |
| Transactions API | http://localhost:8001/docs | — |
| Performances API | http://localhost:8002/docs | — |
| Airflow | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8081 | — |
| Grafana | http://localhost:3000 | admin / admin |

---

