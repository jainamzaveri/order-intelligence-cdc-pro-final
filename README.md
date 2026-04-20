# Order Intelligence CDC Pro

An end-to-end local data engineering project that captures order data changes from PostgreSQL with Change Data Capture (CDC), streams them through Kafka, lands them in DuckDB, transforms them with dbt, and serves business metrics in a Streamlit dashboard.

## Overview

This project simulates an e-commerce analytics pipeline with a more realistic workflow than a simple batch ETL demo.

It includes:
- **PostgreSQL** as the transactional source database
- **Debezium + Kafka Connect** for CDC from Postgres
- **Kafka** as the event transport layer
- **Python consumer** to process change events and build current-state tables in DuckDB
- **DuckDB** as the local analytics store
- **dbt** for curated analytics models
- **Streamlit + Plotly** for the dashboard

The result is a local, portfolio-ready data engineering project that shows both the **technical pipeline** and the **business-facing analytics output**.

---

## Architecture

```text
PostgreSQL
   -> Debezium PostgreSQL Connector
   -> Kafka topics
   -> Python CDC consumer
   -> DuckDB
   -> dbt models
   -> Streamlit dashboard
```

### Data flow
1. Source data is seeded into PostgreSQL.
2. Debezium performs an initial snapshot and then captures inserts, updates, and deletes.
3. Kafka stores CDC events by table/topic.
4. A Python consumer reads Kafka messages and writes:
   - raw CDC events
   - current-state analytical tables
5. dbt builds analytics marts on top of DuckDB.
6. Streamlit displays KPIs, trends, and business insights.

---

## What this project demonstrates

- Change Data Capture from PostgreSQL
- Kafka-based event ingestion
- Raw event logging plus current-state table maintenance
- Local analytics warehousing with DuckDB
- dbt modeling and testing
- Dashboarding for business metrics
- End-to-end orchestration of a multi-tool local stack

This project is designed to be a strong **beginner-to-intermediate data engineering portfolio project**.

---

## Dashboard outputs

The dashboard includes:
- **Orders, Revenue, AOV, Cancel Rate, Repeat Customer Rate** KPI cards
- **Orders and Revenue Trend**
- **Orders by Status**
- **Revenue by City**
- **Top Products by Revenue**
- **Customer Retention by Loyalty Tier**
- **Payment Status Mix**
- **Latest CDC Events** table

> Note: During the initial Debezium snapshot, CDC events may show `op = r`. That means **snapshot read**, not an error. After the snapshot completes, you should also see operations like `c` (create), `u` (update), and `d` (delete).

---

## Data model

### Source tables
- `customers`
- `products`
- `orders`
- `order_items`
- `payments`

### Analytical/current-state tables in DuckDB
- `customers_current`
- `products_current`
- `orders_current`
- `order_items_current`
- `payments_current`
- `raw_cdc_events`

### dbt marts
- `orders_summary`
- `top_products`
- `top_customers`
- `revenue_by_city`
- `customer_retention`
- `hourly_kpis`

---

## Repository structure

```text
.
├── connector/
│   ├── connector.json
│   └── register_connector.py
├── consumer/
│   └── consumer.py
├── dashboard/
│   └── app.py
├── data/
│   └── order_intelligence.duckdb
├── dbt/
│   └── order_intelligence/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── models/
├── notebooks/
│   └── 01_quick_checks.ipynb
├── scripts/
│   ├── seed_postgres.py
│   ├── trigger_changes.py
│   └── run_dbt.py
├── windows/
│   ├── 01_install_packages.bat
│   ├── 02_start_containers.bat
│   ├── 03_seed_source_data.bat
│   ├── 04_register_connector.bat
│   ├── 05_start_consumer.bat
│   ├── 06_trigger_changes.bat
│   ├── 07_run_dbt.bat
│   └── 08_start_dashboard.bat
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Tech stack

- **Python**
- **PostgreSQL**
- **Debezium**
- **Kafka / Kafka Connect**
- **DuckDB**
- **dbt Core**
- **Streamlit**
- **Plotly**
- **Docker Compose**

---

## Getting started

### Prerequisites
Install these first:
- Docker Desktop
- Python 3.10+

### Install Python packages
From the project root:

```bash
pip install -r requirements.txt
```

### Start the containers
```bash
docker compose up -d
```

### Seed the PostgreSQL source database
```bash
python scripts/seed_postgres.py
```

### Register the Debezium connector
```bash
python connector/register_connector.py
```

### Start the CDC consumer
Open a second terminal and keep it running:

```bash
python consumer/consumer.py
```

### Trigger new CDC events
Back in the first terminal:

```bash
python scripts/trigger_changes.py
```

### Run dbt models and tests
```bash
cd dbt/order_intelligence
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### Start the dashboard
From the project root:

```bash
streamlit run dashboard/app.py
```

Open the Streamlit URL shown in the terminal, usually:

```text
http://localhost:8501
```

---

## Recommended run order on Windows

If you are using Windows and want the cleanest workflow:

1. Start containers
2. Seed Postgres
3. Register connector
4. Start consumer
5. Trigger changes
6. Stop consumer
7. Run dbt
8. Start Streamlit dashboard

> Because this project uses a local DuckDB file, it is safest to avoid running the CDC consumer and the dashboard against the same file at the exact same time on Windows.

---

## Expected output

Once everything is running correctly, you should see:
- CDC events flowing from Postgres into Kafka
- raw events persisted in `raw_cdc_events`
- current-state tables updated in DuckDB
- dbt marts built successfully
- a dashboard showing business KPIs and analytical views

---

## Why this project is useful

This project helped me practice:
- designing an end-to-end data pipeline
- understanding CDC operations and event streams
- moving from source data to analytics-ready models
- working with both engineering tools and business-facing outputs

It is a good example of how transactional data can be turned into analytical insights using a modern local stack.

---

## Future improvements

Planned extensions:
- Airflow orchestration
- containerized consumer/dashboard services
- cloud warehouse deployment
- CI/CD and automated tests
- richer anomaly monitoring
- more advanced dbt lineage and documentation

---

## Screenshots

Add screenshots here when publishing to GitHub:
<img width="1914" height="824" alt="image" src="https://github.com/user-attachments/assets/22031469-e7bf-41d0-9b4b-fdbcdb458a7a" />

<img width="1569" height="904" alt="image (1)" src="https://github.com/user-attachments/assets/4e834207-3b69-49c6-9810-1334ff5ce9f4" />


<img width="1522" height="629" alt="image (2)" src="https://github.com/user-attachments/assets/1af5a676-096b-4bf1-ac7c-f81edac7ab86" />

- architecture diagram


---

## License

This project is for learning and portfolio use.
