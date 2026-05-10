# E-Commerce Clickstream & Inventory Watch Pipeline



An end-to-end Big Data pipeline implementing a Kappa-style architecture to ingest, process, and analyze real-time e-commerce user events. Developed as part of the Applied Big Data Engineering Mini Project Assessment (Scenario 3).

---

## Project Objective
The objective of this project is to simulate an e-commerce platform tracking user activity to monitor stock interest dynamically. The pipeline handles:
- **High-throughput Ingestion**: Streaming simulated clickstream events into Kafka.
- **Real-Time Stream Processing**: Using Spark to monitor products experiencing High Interest but Low Conversion (Views > 10, Purchases < 5) within a 10-minute sliding window, immediately logging Flash Sale alerts.
- **Batch Orchestration**: Using Airflow to execute a periodic job (every 1 minute) that segments users into "Window Shoppers" vs "Buyers" and generates analytical conversion reports.

---

## Architecture & Technologies
- **Ingestion**: Apache Kafka
- **Stream Processing**: Apache Spark Structured Streaming
- **Storage Sink / Data Warehouse**: PostgreSQL
- **Batch Orchestration**: Apache Airflow
- **Containerization**: Docker Compose

> A visual representation of the architecture can be found in `docs/architecture_diagram.md`.

---

## Repository Structure
Following standard data engineering principles, the codebase is modularly structured:

```text
/
├── src/                               # Application source code
│   ├── producer/                      # Python mock data generator
│   │   └── ecommerce_producer.py
│   └── streaming/                     # PySpark structured streaming 
│       └── spark_processing.py
├── orchestration/                     # Apache Airflow definitions
│   └── dags/
│       └── daily_segmentation_dag.py
├── infrastructure/                    # Environment provisioning
│   ├── docker-compose.yml
│   └── database/
│       └── init_db.sql
├── docs/                              # Project documentation
│   ├── Project_Report_Dulshan.md
│   └── architecture_diagram.md
├── output/                            # Generated Airflow analysis reports
├── .env                               # Hidden Secrets File
├── .gitignore
└── README.md
```

---

## Setup & Installation

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/) and Docker Compose installed.
- Python 3.8 or higher.
- Required python packages (found in `src/producer/requirements.txt` or installable via PIP):
  ```bash
  pip install kafka-python pyspark psycopg2-binary python-dotenv
  ```

### Environment Initialization
1. Ensure your `.env` file exists in the root directory (it stores the PostgreSQL and Kafka connection strings).
2. Start the Docker containers containing Kafka, Spark, PostgreSQL, and Airflow:
   ```bash
   cd infrastructure
   docker compose --env-file ../.env up -d
   ```
   *Note: Upon startup, `init_db.sql` automatically mounts and provisions the required PG schemas.*

---

## Execution Guide

### 1. Start the Data Generator
Run the mock producer to begin simulating user website traffic in real-time. This script pumps JSON payloads directly into the `ecommerce-events` Kafka topic.
*(Keep this running in an active terminal window)*
```bash
KAFKA_BROKER=localhost:9092 python3 src/producer/ecommerce_producer.py
```

### 2. Begin Stream Processing
Start the Apache Spark Structured Streaming job. The script will securely connect to Kafka, parse the stream, apply a 10-minute watermark/sliding window, and write identified real-time flash-sale alerts into PostgreSQL.
```bash
python3 src/streaming/spark_processing.py
```

### 3. Orchestrate Batch Analytics
1. Navigate to the Apache Airflow Web GUI at `http://localhost:8081`. 
2. Login using the default credentials (`admin` / `admin`).
3. Locate the `daily_user_segmentation` DAG.
4. Manually trigger the DAG to simulate the nightly run.
5. Once marked `Success`, verify the results inside the `/output/` folder on your local machine. You will see newly generated `.csv` and `.txt` files containing categorized analytics.

---

**Author**: T.D Rasindu Dulshan Siriwardhana
