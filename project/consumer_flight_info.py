from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

from kafka import KafkaConsumer
import json
import pprint
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-submit'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Consumer_flight_info")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/abdelhakabdelhak/kafka/project/checkpoint")

    # Kafka broker address
    bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

    # Kafka topic name
    topic_name = 'flight_info'

    # Define the schema for the JSON data
    schema = StructType([
        StructField("age", IntegerType(), True),
        StructField("aircraft_icao", StringType(), True),
        StructField("airline_iata", StringType(), True),
        StructField("airline_icao", StringType(), True),
        StructField("airline_name", StringType(), True),
        StructField("alt", IntegerType(), True),
        StructField("arr_actual", StringType(), True),
        StructField("arr_actual_utc", StringType(), True),
        StructField("arr_baggage", StringType(), True),
        StructField("arr_city", StringType(), True),
        StructField("arr_country", StringType(), True),
        StructField("arr_delayed", StringType(), True),
        StructField("arr_estimated", StringType(), True),
        StructField("arr_estimated_utc", StringType(), True),
        StructField("arr_gate", StringType(), True),
        StructField("arr_iata", StringType(), True),
        StructField("arr_icao", StringType(), True),
        StructField("arr_name", StringType(), True),
        StructField("arr_terminal", StringType(), True),
        StructField("arr_time", StringType(), True),
        StructField("arr_time_ts", IntegerType(), True),
        StructField("arr_time_utc", StringType(), True),
        StructField("built", StringType(), True),
        StructField("cs_airline_iata", StringType(), True),
        StructField("cs_flight_iata", StringType(), True),
        StructField("cs_flight_number", StringType(), True),
        StructField("delayed", StringType(), True),
        StructField("dep_actual", StringType(), True),
        StructField("dep_actual_utc", StringType(), True),
        StructField("dep_city", StringType(), True),
        StructField("dep_country", StringType(), True),
        StructField("dep_delayed", StringType(), True),
        StructField("dep_estimated", StringType(), True),
        StructField("dep_estimated_utc", StringType(), True),
        StructField("dep_gate", StringType(), True),
        StructField("dep_iata", StringType(), True),
        StructField("dep_icao", StringType(), True),
        StructField("dep_name", StringType(), True),
        StructField("dep_terminal", StringType(), True),
        StructField("dep_time", StringType(), True),
        StructField("dep_time_ts", IntegerType(), True),
        StructField("dep_time_utc", StringType(), True),
        StructField("dir", IntegerType(), True),
        StructField("duration", IntegerType(), True),
        StructField("engine", StringType(), True),
        StructField("engine_count", StringType(), True),
        StructField("eta", IntegerType(), True),
        StructField("flag", StringType(), True),
        StructField("flight_iata", StringType(), True),
        StructField("flight_icao", StringType(), True),
        StructField("flight_number", StringType(), True),
        StructField("hex", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("model", StringType(), True),
        StructField("msn", StringType(), True),
        StructField("percent", IntegerType(), True),
        StructField("reg_number", StringType(), True),
        StructField("speed", IntegerType(), True),
        StructField("squawk", StringType(), True),
        StructField("status", StringType(), True),
        StructField("type", StringType(), True),
        StructField("updated", IntegerType(), True),
        StructField("utc", StringType(), True),
        StructField("v_speed", DoubleType(), True),
    ])
                            
    df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", ",".join(bootstrap_servers))
            .option("subscribe", topic_name)
            .option("startingOffsets", "latest")
            .load()
    )    

    # Convert the binary data from Kafka into a string representation
    df = df.selectExpr("CAST(value AS STRING)")
    df = df.withColumn("value", F.from_json(F.col("value"), schema))

    # Select the columns from the DataFrame
    extracted_df = df.select("value.*")

    extracted_df = extracted_df.withColumn("timestamp", F.current_timestamp())

    # Apply the watermark on the "timestamp" column
    extracted_df_with_watermark = extracted_df.withWatermark("timestamp", "1 minute")

    # Group by airline_name and window by 5 minutes, then calculate the specified metrics
    aggregated_df = extracted_df_with_watermark.groupBy(
        F.window("timestamp", "1 minute"), "airline_name"
    ).agg(
        F.count("reg_number").alias("num_aircrafts"),
        F.count(F.when(F.col("type") == "landplane", 1)).alias("num_landplanes"),
        F.count(F.when(F.col("type") == "tiltrotor", 1)).alias("num_tiltrotors"),
        F.count(F.when(F.col("type") == "helicopter", 1)).alias("num_helicopters"), 
        F.max("age").alias("max_aircraft_age"),
        F.min("age").alias("min_aircraft_age"),
        F.avg("age").alias("avg_aircraft_age"),
        F.max("duration").alias("max_flight_dur"),
        F.min("duration").alias("min_flight_dur"),
        F.avg("duration").alias("avg_flight_dur"),
        F.count(F.when(F.col("status") == "scheduled", 1)).alias("num_scheduled_flights"),
        F.count(F.when(F.col("status") == "en-route", 1)).alias("num_en-route_flights"),
        F.count(F.when(F.col("status") == "landed", 1)).alias("num_landed_flights"), 
    )

    final_df = aggregated_df.select('airline_name', 'num_aircrafts', 'num_landplanes', 'num_tiltrotors', 'num_helicopters', 'max_aircraft_age', 'min_aircraft_age', 'avg_aircraft_age', 'max_flight_dur', 'min_flight_dur', 'avg_flight_dur', 'num_scheduled_flights', 'num_en-route_flights', 'num_landed_flights')

    output_path = "/home/abdelhakabdelhak/kafka/project/files/airlines_stats.csv"
    
    # Write the extracted DataFrame to the console
    query = final_df.writeStream.format("console").outputMode("complete").start()

    """query = aggregated_df.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094").option("topic", "flight_info_stats").start()

    # The output path for the CSV file
    output_path = "/home/abdelhakabdelhak/kafka/project/files/airlines_stats.csv"

    # Write the extracted DataFrame to the console
    query = aggregated_df.writeStream \
        .format("csv").option("header", "true").option("path", output_path).outputMode("complete").start()"""

    # Await termination of the query
    query.awaitTermination()