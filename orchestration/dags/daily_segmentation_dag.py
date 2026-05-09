from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os

db_config = {
    "host": "postgres",
    "database": os.environ.get("POSTGRES_DB", "bigdata_db"),
    "user": os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow_password")
}

def analyze():
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    cur.execute("""
        WITH user_purchases AS (
            SELECT user_id, 
                   SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases
            FROM raw_events
            GROUP BY user_id
        )
        SELECT user_id, 
               CASE WHEN total_purchases > 0 THEN 'Buyer' ELSE 'Window Shopper' END as segment
        FROM user_purchases;
    """)
    segs = cur.fetchall()
    
    cur.execute("""
        SELECT product_id, COUNT(*) as views
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
                WHEN CAST(SUBSTRING(product_id FROM 6) AS INTEGER) <= 5 THEN 'Laptops'
                WHEN CAST(SUBSTRING(product_id FROM 6) AS INTEGER) <= 10 THEN 'Mobiles'
                WHEN CAST(SUBSTRING(product_id FROM 6) AS INTEGER) <= 15 THEN 'Audio'
                ELSE 'Accessories'
            END as category,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases
        FROM raw_events
        GROUP BY 1;
    """)
    rates = cur.fetchall()
    
    cur.close()
    conn.close()

    os.makedirs("/opt/airflow/reports", exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    with open(f"/opt/airflow/reports/daily_{ts}.txt", 'w') as f:
        f.write("summary \n\ntop 5 products:\n")
        for p in tops:
            f.write(f"- {p[0]}: {p[1]} views\n")
            
        f.write("\nusers (sample of 10):\n")
        for s in segs[:10]:
            f.write(f"- {s[0]}: {s[1]}\n")
        f.write(f"total: {len(segs)}\n")
        
    with open(f"/opt/airflow/reports/conv_{ts}.csv", 'w') as f:
        f.write("product,views,purchases,rate\n")
        for r in rates:
            pid, v, p = r
            rate = p/v if v > 0 else 0
            f.write(f"{pid},{v},{p},{rate:.4f}\n")

with DAG(
    'daily_user_segmentation',
    default_args={'start_date': datetime(2023, 1, 1), 'retries': 1},
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    PythonOperator(task_id='run_metrics', python_callable=analyze)
