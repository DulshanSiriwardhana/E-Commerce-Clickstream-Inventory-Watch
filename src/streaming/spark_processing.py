import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, expr, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

load_dotenv()

SPARK_VERSION = "3.4.0"

kafka_broker = os.environ.get("KAFKA_BROKER", "kafka:29092")
kafka_topic = os.environ.get("KAFKA_TOPIC", "ecommerce-events")
pg_url = f"jdbc:postgresql://postgres:5432/{os.environ.get('POSTGRES_DB', 'bigdata_db')}"
pg_props = {
    "user": os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow_password"),
    "driver": "org.postgresql.Driver"
}

spark_packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
    "org.postgresql:postgresql:42.6.0"
]

spark = SparkSession.builder \
    .appName("Clickstream") \
    .config("spark.jars.packages", ",".join(spark_packages)) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id",   StringType(), True),
    StructField("user_id",    StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp",  StringType(), True),
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .drop("timestamp")

def write_raw(batch_df, epoch_id):
    batch_df.withColumn("inserted_at", current_timestamp()) \
        .write.jdbc(url=pg_url, table="raw_events", mode="append", properties=pg_props)

raw_query = parsed \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_raw) \
    .start()

win_df = parsed \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "10 minutes", "1 minute"),
        col("product_id")
    ) \
    .agg(
        expr("sum(case when event_type = 'view'     then 1 else 0 end)").alias("views"),
        expr("sum(case when event_type = 'purchase' then 1 else 0 end)").alias("purchases")
    )

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

alerts_query = win_df.writeStream.outputMode("update").foreachBatch(process_batch).start()

spark.streams.awaitAnyTermination()
