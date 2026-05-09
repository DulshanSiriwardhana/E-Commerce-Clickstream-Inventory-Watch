# Applied Big Data Engineering - Mini Project Assessment

**Student Name**: T.D Rasindu Dulshan Siriwardhana
**Scenario Selected**: Scenario 3 - E-Commerce Clickstream & Inventory Watch

---

## 1. Justification of the Technical Stack

For this e-commerce analytics scenario, handling both real-time streams and daily batch processing needs a solid architecture. I went with a Kappa-style architecture where Kafka acts as the main entry point for everything. Here are the tools I chose:

**Apache Kafka (Ingestion):**
Kafka is great for clickstream data because of its high throughput. Since an e-commerce platform has a lot of events happening (views, adds to cart, purchases), we need something fast. Kafka partitions also help keep the events ordered properly for downstream apps.

**Apache Spark Structured Streaming (Real-Time Processing):**
I picked Spark Structured Streaming over Storm because it's easier to use the Dataframe/SQL API. Also, it has built-in support for sliding windows. In this scenario, we need to find products with high views but low purchases in a 10-minute window, and Spark handles this stateful aggregation easily.

**PostgreSQL (Storage & Sink):**
PostgreSQL acts as both the database for saving real-time `alerts` and as a data warehouse to keep `raw_events`. Although HDFS or S3 might be better for huge datasets, PostgreSQL works perfectly here for storing the data and running the daily SQL batch queries using Airflow.

**Apache Airflow (Orchestration):**
I used Airflow instead of normal cron jobs to run the daily batch segmentations. Airflow makes it easy to schedule the job, handle dependencies, and easily check if the job ran successfully or failed. 

## 2. Event Time vs Processing Time

A big challenge with analyzing clickstream data is differentiating when the user actually clicked (Event Time) vs when the data was processed by Spark (Processing Time).

In my code, the mock producer assigns a `timestamp` field to every JSON event it creates. This represents the actual **Event Time**.

If I aggregated the data based on **Processing Time**, network delays could cause the events to fall into the wrong 10-minute window, ruining the logic for triggering flash sales. So, I configured Spark to group the data by the parsed `timestamp` column.

To handle data that arrives late, I used **Watermarking**. Adding `.withWatermark("timestamp", "1 minute")` tells Spark to keep the window state open for one extra minute just in case older events arrive slightly late due to network lag.

## 3. Ethics and Data Governance

Tracking e-commerce clickstream behavior has some serious privacy issues that need to be addressed.

**Privacy Implications (User Profiling):** 
The dataset logs every click down to the specific `user_id`. While this helps the business with targeted sales, it basically builds an invasive profile of the user’s browsing habits and schedules. The daily Airflow job specifically tags users as 'Window Shoppers' or 'Buyers', which feels a bit invasive if they don't know it's happening.

**Data Governance Setup:**
To comply with data privacy laws, we should apply some governance rules to this pipeline:

1. **Anonymization:** We shouldn't use real PII (Personally Identifiable Information) in the `user_id` field. It should be hashed or encrypted before it enters Kafka.
2. **Data Retention:** Storing all the granular `raw_events` forever is risky. We need a TTL (Time-To-Live) policy to delete old raw data from PostgreSQL after a certain number of days, only keeping aggregated reports.
3. **Purpose Limitation:** Access to the reports should be strictly controlled, ensuring they are only used for inventory watch and discounts, and not sold to third-party ad networks or used for predatory pricing.
