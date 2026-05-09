# Applied Big Data Engineering - Mini Project Assessment Setup Guide

This repository contains the source code for the End-to-End E-Commerce Clickstream Big Data Pipeline (Scenario 3). Following industry best practices, the project is structured in a modular format.

## Repository Structure

```text
/Big_Data
├── src/                               # Application source code
│   ├── producer/                      # Python mock data generator
│   │   └── ecommerce_producer.py
│   └── streaming/                     # PySpark structured streaming 
│       └── spark_processing.py
├── orchestration/                     # Apache Airflow definitions
│   └── dags/
│       └── daily_segmentation_dag.py
├── infrastructure/                    # Docker & Database setup
│   ├── docker-compose.yml
│   └── database/
│       └── init_db.sql
├── docs/                              # Project documentation
│   ├── Project_Report_Dulshan.md
│   └── architecture_diagram.md
├── output/                            # Airflow analysis reports (.csv, .txt)
├── README.md
└── .gitignore
```

## Prerequisites
- Docker & Docker Compose installed.
- Python 3.8+ installed locally.
- `pip install kafka-python pyspark psycopg2-binary python-dotenv`

## Getting Started

1. **Spin up the Infrastructure**
Navigate to the `infrastructure/` directory and run:
```bash
cd infrastructure
docker compose --env-file ../.env up -d
```
This will start Zookeeper, Kafka, PostgreSQL, Spark Master/Worker, and Apache Airflow.
*The `init_db.sql` automatically configures the `raw_events` and `alerts` tables inside PostgreSQL.*

2. **Start the Data Producer**
Run the streaming mock data generator from the root to begin pushing clickstream data naticely into Kafka:
```bash
python src/producer/ecommerce_producer.py
```
*Note: Keep this running in an open terminal to mimic real-time users.*

3. **Start Stream Processing**
Run the PySpark script to begin consuming from Kafka:
```bash
python src/streaming/spark_processing.py
```
This calculates sliding window aggregations in real time and writes to PostgreSQL.

4. **Trigger Airflow Orchestration**
- Go to `http://localhost:8081` in your browser to view the Airflow UI (login: `admin` / `admin`).
- Locate the `daily_user_segmentation` DAG.
- Unpause and manually trigger it.
- After completion, check your local `output/` folder for the newly generated `.txt` and `.csv` files showing User Segmentation, Top 5 Products, and Category Conversion Rates.
