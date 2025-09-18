from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col
from cassandra.cluster import Cluster
import logging

# creates a spark named SparkDataStreaming
def create_spark_connection():
    try:
        # uses 'jars' to load 2 connector packages: 
        # 1 to read/write cassandra db
        # 2 to read kafka topics as dataframes
        spark = (
            SparkSession.builder
                .appName('SparkDataStreaming')
                .config(
                    'spark.jars.packages',
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
                )
                .config('spark.cassandra.connection.host', 'cassandra')
                .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        logging.error(f"couldnt create spark session: {e}")
        return None

# reads from the users_created topic as a dataframe
def connect_to_kafka(spark_conn):
    try:
        df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load() # load() creates a DataFrame with Kafka’s raw data.
        return df
    except Exception as e:
        logging.error(f"could not read from kafka: {e}")
        return None
# result: dataframe read from kafka topic 'users_created looks like this
# key | value | topic | partition | offset | timestamp | ...


# takes in the streamed df, and transforms it into a strcuted df + defines the schema of the data
def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), False)
    ])

    # casts kafka's value field from bytes (streamed as bytes) to string
    # REMEMBER THAT big data is usally transmitted as bytes
    return spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
# result: dataframe passed in as argument now only has the values:
# first_name | last_name | email


# connects to cassandra db (obviously running on docker)
def create_cassandra_connection():
    try:
        cluster = Cluster(['cassandra'])
        return cluster.connect()
    except Exception as e:
        logging.error(f"Could not connect to Cassandra: {e}")
        return None

# function to create a cassandra keyspace (basically a database) called spark_stream
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    logging.info("Keyspace created successfully!")
# creates replication for high availability (REMEMBER CAP THEOREM)
# If one Cassandra node goes down, another node with the replicated data can serve requests.
# Without replication, if your only node fails, your data is gone.


# creates the table in the keyspace (database)
# we ownself determine p.key as last_name and email!
def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            PRIMARY KEY (last_name, email) 
        )
    """) 
    logging.info("Table created successfully!")


# runs the whole program for spark stream consuming data from kafka and writing to cassandra
def main():
    spark_conn = create_spark_connection()
    if spark_conn:
        spark_df = connect_to_kafka(spark_conn) # connect to kafka
        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df) # consumes 'user_created' from kafka
            session = create_cassandra_connection() # connect to cassandra
            if session:
                create_keyspace(session) # create keyspace
                create_table(session) # create table

                # tells Spark Structured Streaming to write results continuously into Cassandra.
                streaming_query = (selection_df.writeStream
                                   .format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .option('keyspace', 'spark_streams')
                                   .option('table', 'created_users')
                                   .start())
                streaming_query.awaitTermination() # waits for the streaming query to terminate

# result: For every new Kafka message → Spark parses it → inserts into Cassandra table.

if __name__ == "__main__":
    main()