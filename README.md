# Real-Time Streaming Data Pipeline (Kafka + Spark + Cassandra)

## Overview
This experimental project simulates a streaming data pipeline built with:

- **Apache Kafka** – message broker for streaming events  
- **Apache Spark Streaming** – real-time data processing  
- **Apache Cassandra** – distributed NoSQL database for storage  
- **Apache Airflow** – task scheduler for orchestrating the data producer  

The pipeline fetches random user data from an API, streams it into Kafka, processes it with Spark, and stores it in Cassandra. All the above containers run in Docker containers.

---

## 1. Pipeline Workflow

### 1.1 Kafka Producer (automated by Airflow DAG)
- Airflow runs the `stream_data()` task **once per day**.
- That task:
  - Calls the **RandomUser API** (`requests.get(...)`).
  - Extracts `first_name`, `last_name`, and `email`.
  - Sends the formatted JSON payload to the Kafka topic **`users_created`**.
- The loop runs for **60 seconds**, so for one minute each day, new user data is streamed into Kafka.

### 1.2 Kafka (Broker)
- Acts as a **buffer**.
- Stores all incoming events in the topic **`users_created`**.
- Keeps them durable and makes them available for consumers like Spark.

### 1.3 Spark Structured Streaming (Consumer)
- Connects to Kafka
- Continuously reads events from topic **`users_created`**.
- Parses the JSON string into a proper **DataFrame** with columns:
  - `first_name`
  - `last_name`
  - `email`

### 1.4 Cassandra db
- Spark writes those structured rows into Cassandra table:
  - **Keyspace** → `spark_streams`
  - **Table** → `created_users`
  - **Fields** → `first_name`, `last_name`, `email`

---

## 2. Streaming Characteristics

### ✅ Real-Time Capabilities
- Kafka → Spark → Cassandra integration is considred streaming.
- Spark enable continuous ingestion and processing.
- As soon as Kafka receives a message, Spark processes it and writes to Cassandra with **low latency**.

### ⚠️ Current Limitation
- The **producer** runs only once per day for 60 seconds (scheduled via Airflow).  
- This means the pipeline processes data in short bursts rather than **24/7 continuous streaming**.

## 3. Run the project
1. start up Docker on you machine
2. run `docker compose up -d`
3. run `docker ps` there should be 11 containers running
4. Access Airflow Web UI (for monitoring DAGs) at http://localhost:8080 
<img width="1903" height="400" alt="image" src="https://github.com/user-attachments/assets/a6d97813-071d-4dca-bf8e-163bac475ec1" />

5. Manually trigger the Airflow DAG (just to see the results)
6. Access Spark Master UI at http://localhost:9090 to check its running
<img width="1920" height="483" alt="image" src="https://github.com/user-attachments/assets/9ea7c450-7384-4119-bb40-33ec3a3930ee" />

7. run `docker compose exec -it cassandra_db cqlsh -u cassandra -p cassandra`
8. run `USE spark_streams; SELECT * FROM created_users;`
<img width="438" height="184" alt="image" src="https://github.com/user-attachments/assets/d6e6e240-f81a-4afc-9b22-0a34ef334c39" />



