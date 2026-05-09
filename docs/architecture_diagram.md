# E-Commerce Clickstream Big Data Pipeline Architecture

Follow this Flow Architecture for the E-Commerce Clickstream Mini-Project.

```mermaid
graph TD
    %% Producers
    P1[Python Producer Script<br>ecommerce_producer.py] -->|JSON Events| K[(Apache Kafka)]
    
    %% Stream Processing
    K --> S[Apache Spark<br>Structured Streaming]
    
    %% Lambda/Kappa routing
    S -->|Window Aggregations<br>& Alert Logic| Postgres[(PostgreSQL DB)]
    S -->|Raw Events Appended| Postgres
    
    %% Orchestration & Batch
    Airflow[Apache Airflow<br>daily_segmentation_dag] -->|Nightly Query &<br>Extract from raw_events| Postgres
    
    %% Final Outputs
    Airflow -->|Write| Output1[Text Report<br>Window Shoppers vs Buyers<br>Top 5 Viewed]
    Airflow -->|Write| Output2[CSV Analytics<br>Conversion Rates]
    
    %% Styling
    classDef datastore fill:#f9f,stroke:#333,stroke-width:2px;
    classDef processing fill:#bbf,stroke:#333,stroke-width:2px;
    classDef output fill:#bfb,stroke:#333,stroke-width:2px;
    
    class K,Postgres datastore;
    class S,Airflow processing;
    class Output1,Output2 output;
```

### Components Summary:
1. **Producer**: Python simulating IoT/Clickstream scaling.
2. **Kafka**: Ingests JSON payloads into `ecommerce-events` topic.
3. **Spark**: Filters, applies 10-minute sliding window, outputs metrics & alerts.
4. **PostgreSQL**: Serves as persistent sink (data warehouse format) for `raw_events` and `alerts`.
5. **Airflow**: Nightly DAG querying PostgreSQL to calculate user segmentations and report outputs.
