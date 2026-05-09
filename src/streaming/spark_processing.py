import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

import os
from dotenv import load_dotenv

load_dotenv()

pg_url = f"jdbc:postgresql://postgres:5432/{os.environ.get('POSTGRES_DB', 'bigdata_db')}"
pg_props = {
    "user": os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow_password"),
    "driver": "org.postgresql.Driver"
}

def process_batch(df, epoch_id):
    alerts = df.filter((col("views") > 100) & (col("purchases") < 5)) \
               .withColumn("message", expr("concat('Flash Sale idea for ', product_id)"))
    
    if not alerts.isEmpty():
        alerts.select(
            col("product_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("views"),
            col("purchases"),
            col("message")
        ).write.jdbc(url=pg_url, table="alerts", mode="append", properties=pg_props)
        alerts.show(truncate=False)


spark = SparkSession.builder.appName("Clickstream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce-events") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

raw_query = parsed.withColumn("inserted_at", current_timestamp()) \
    .withColumnRenamed("timestamp", "event_time") \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda d, e: d.write.jdbc(url=pg_url, table="raw_events", mode="append", properties=pg_props)) \
    .start()


win_df = parsed \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "10 minutes", "1 minute"), col("product_id")) \
    .agg(
        expr("sum(case when event_type = 'view' then 1 else 0 end)").alias("views"),
        expr("sum(case when event_type = 'purchase' then 1 else 0 end)").alias("purchases")
    )

alerts_query = win_df.writeStream.outputMode("update").foreachBatch(process_batch).start()

spark.streams.awaitAnyTermination()
