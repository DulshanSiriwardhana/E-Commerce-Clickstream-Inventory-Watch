import os
import psycopg2
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

REPORTS_DIR = "/opt/airflow/reports"

db_config = {
    "host":     "postgres",
    "database": os.environ.get("POSTGRES_DB",       "bigdata_db"),
    "user":     os.environ.get("POSTGRES_USER",     "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow_password"),
}


def analyze(**context):
    conn = psycopg2.connect(**db_config)
    cur  = conn.cursor()

    cur.execute("""
        WITH user_purchases AS (
            SELECT user_id,
                   SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases
            FROM raw_events
            GROUP BY user_id
        )
        SELECT user_id,
               CASE WHEN total_purchases > 0 THEN 'Buyer' ELSE 'Window Shopper' END AS segment
        FROM user_purchases;
    """)
    segs = cur.fetchall()

    cur.execute("""
        SELECT product_id, COUNT(*) AS views
        FROM raw_events
        WHERE event_type = 'view'
        GROUP BY product_id
        ORDER BY views DESC
        LIMIT 5;
    """)
    tops = cur.fetchall()

    cur.execute("""
        SELECT
            CASE
                WHEN CAST(SUBSTRING(product_id FROM 6) AS INTEGER) <= 5  THEN 'Laptops'
                WHEN CAST(SUBSTRING(product_id FROM 6) AS INTEGER) <= 10 THEN 'Mobiles'
                WHEN CAST(SUBSTRING(product_id FROM 6) AS INTEGER) <= 15 THEN 'Audio'
                ELSE 'Accessories'
            END AS category,
            SUM(CASE WHEN event_type = 'view'     THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
        FROM raw_events
        GROUP BY 1;
    """)
    rates = cur.fetchall()

    cur.close()
    conn.close()

    os.makedirs(REPORTS_DIR, exist_ok=True)
    ts = pendulum.now("UTC").strftime("%Y%m%d_%H%M%S")

    with open(f"{REPORTS_DIR}/daily_{ts}.txt", "w") as f:
        f.write("=== Daily Summary ===\n\nTop 5 Products by Views:\n")
        for p in tops:
            f.write(f"  - {p[0]}: {p[1]} views\n")

        f.write("\nUser Segments (sample of 10):\n")
        for s in segs[:10]:
            f.write(f"  - {s[0]}: {s[1]}\n")
        f.write(f"\nTotal users analysed: {len(segs)}\n")

    with open(f"{REPORTS_DIR}/conversion_{ts}.csv", "w") as f:
        f.write("category,views,purchases,conversion_rate\n")
        for r in rates:
            cat, v, p = r
            rate = p / v if v > 0 else 0
            f.write(f"{cat},{v},{p},{rate:.4f}\n")

    print(f"Reports written to {REPORTS_DIR} with timestamp {ts}", flush=True)


default_args = {
    "owner":          "airflow",
    "start_date":     pendulum.datetime(2024, 1, 1, tz="UTC"),
    "retries":        2,
    "retry_delay":    timedelta(minutes=5),
}

with DAG(
    dag_id="daily_user_segmentation",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    tags=["ecommerce", "segmentation"],
) as dag:

    run_metrics = PythonOperator(
        task_id="run_metrics",
        python_callable=analyze,
    )
