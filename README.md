# kafka-spark-streaming

## Full Data Flow

### 1. **Kafka Producer (automated by Airflow DAG)**

- Airflow runs the `stream_data()` task **once per day**.
- That task:
  - Calls the **RandomUser API** (`requests.get(...)`).
  - Extracts `first_name`, `last_name`, and `email`.
  - Sends the formatted JSON payload to the Kafka topic **`users_created`**.
- The loop runs for **60 seconds**, so for one minute each day, new user data is streamed into Kafka.

---

### 2. **Kafka (Broker)**

- Acts as a **buffer / message bus**.
- Stores all incoming events in the topic **`users_created`**.
- Keeps them durable and makes them available for consumers like Spark.

---

### 3. **Spark Structured Streaming (Consumer)**

- Connects to Kafka (`broker:29092`).
- Continuously reads events from topic **`users_created`**.
- Parses the JSON string into a proper **DataFrame** with columns:
  - `first_name`
  - `last_name`
  - `email`

---

### 4. **Cassandra (Sink / Storage)**

- Spark writes those structured rows into Cassandra table:
  - **Keyspace** → `spark_streams`
  - **Table** → `created_users`
  - **Fields** → `first_name`, `last_name`, `email`
