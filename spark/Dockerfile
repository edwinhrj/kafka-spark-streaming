FROM bitnami/spark:3.4.1

# Install Python Cassandra driver compatible with Python 3.12
RUN pip install --no-cache-dir "cassandra-driver>=3.29.0"

# Prefer asyncio reactor and skip C extensions (libev)
ENV CASSANDRA_DRIVER_REACTOR=asyncio
ENV CASS_DRIVER_NO_CYTHON=1

# Copy the Spark streaming app
COPY spark/spark_stream.py /opt/app/spark_stream.py

# Submit the job to the cluster master with Kafka + Cassandra connectors
CMD ["/opt/bitnami/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1", "/opt/app/spark_stream.py"]
