from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import pandas as pd

from kafka import KafkaConsumer
import json
import pprint
import os
import datetime

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-submit'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'

"""
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'
"""

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Consumer_schedules")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/abdelhakabdelhak/kafka/project/checkpoint")

    # Kafka broker address
    bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

    # Kafka topic name
    topic_name = 'schedules'

    # Define the schema for the response data
    schema = StructType([
        StructField("aircraft_icao", StringType(), True),
        StructField("airline_iata", StringType(), True),
        StructField("airline_icao", StringType(), True),
        StructField("arr_baggage", StringType(), True),
        StructField("arr_delayed", IntegerType(), True),
        StructField("arr_estimated", StringType(), True),
        StructField("arr_estimated_ts", IntegerType(), True),
        StructField("arr_estimated_utc", StringType(), True),
        StructField("arr_gate", StringType(), True),
        StructField("arr_iata", StringType(), True),
        StructField("arr_icao", StringType(), True),
        StructField("arr_terminal", StringType(), True),
        StructField("arr_time", StringType(), True),
        StructField("arr_time_ts", IntegerType(), True),
        StructField("arr_time_utc", StringType(), True),
        StructField("cs_airline_iata", StringType(), True),
        StructField("cs_flight_iata", StringType(), True),
        StructField("cs_flight_number", StringType(), True),
        StructField("delayed", IntegerType(), True),
        StructField("dep_delayed", IntegerType(), True),
        StructField("dep_estimated", StringType(), True),
        StructField("dep_estimated_ts", IntegerType(), True),
        StructField("dep_estimated_utc", StringType(), True),
        StructField("dep_gate", StringType(), True),
        StructField("dep_iata", StringType(), True),
        StructField("dep_icao", StringType(), True),
        StructField("dep_terminal", StringType(), True),
        StructField("dep_time", StringType(), True),
        StructField("dep_time_ts", IntegerType(), True),
        StructField("dep_time_utc", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("flight_iata", StringType(), True),
        StructField("flight_icao", StringType(), True),
        StructField("flight_number", StringType(), True),
        StructField("status", StringType(), True)
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

    # Explode the array of JSON objects into individual rows
    exploded_df = df.select(F.explode(F.from_json(F.col("value"), ArrayType(schema))).alias("data"))

    # Select the columns from the exploded DataFrame
    extracted_df = exploded_df.select("data.*")
    extracted_df = extracted_df.withColumn("timestamp", F.current_timestamp())


    # Apply the watermark on the "timestamp" column
    extracted_df_with_watermark = extracted_df.withWatermark("timestamp", "1 minute")

    # Define the format of the timestamp in the 'dep_time' column
    timestamp_format = "yyyy-MM-dd HH:mm"

    # Transform the 'dep_time' column from strings to timestamps
    df_with_timestamp = extracted_df_with_watermark.withColumn("dep_time", F.to_timestamp("dep_time", format=timestamp_format))

    # Group by dep_iata and window by 5 minutes, then calculate the specified metrics
    aggregated_df = df_with_timestamp.groupBy(
        F.window("timestamp", "1 minute"), "dep_iata"
    ).agg(
        F.count("flight_iata").alias("num_flights"),
        F.count(F.when(F.col("status") == "scheduled", 1)).alias("num_flights_scheduled"),
        F.count(F.when(F.col("status") == "cancelled", 1)).alias("num_flights_cancelled"),
        F.count(F.when(F.col("status") == "active", 1)).alias("num_flights_active"),
        F.count(F.when(F.col("status") == "landed", 1)).alias("num_flights_landed"), 
        F.collect_list("dep_time").alias("timestamps_list")
    )


    # UDF to calculate the closest timestamp to the current time
    def find_closest_timestamp(timestamps_list):
        current_time = datetime.datetime.now()
        closest_timestamp = min(timestamps_list, key=lambda x: (current_time - x))
        return closest_timestamp

    # Register UDF
    find_closest_timestamp_udf = F.udf(find_closest_timestamp, TimestampType())

    # UDF to calculate the furthest timestamp to the current time
    def find_furthest_timestamp(timestamps_list):
        current_time = datetime.datetime.now()
        furthest_timestamp = max(timestamps_list, key=lambda x: (current_time - x))
        return furthest_timestamp

    # Register UDF
    find_furthest_timestamp_udf = F.udf(find_furthest_timestamp, TimestampType())

    # Apply the UDF to calculate the closest timestamp for each group
    aggregated_df = aggregated_df.withColumn(
        "closest_flight",
        find_closest_timestamp_udf("timestamps_list")
    )

    # Apply the UDF to calculate the furthest timestamp for each group
    aggregated_df = aggregated_df.withColumn(
        "furthest_flight",
        find_furthest_timestamp_udf("timestamps_list")
    )

    final_df = aggregated_df.select('dep_iata', 'num_flights', 'num_flights_scheduled', 'num_flights_cancelled', 'num_flights_active', 'num_flights_landed', 'closest_flight', 'furthest_flight')

    final_df = final_df.withColumn("timestamp", F.current_timestamp())

    # Apply the watermark on the "timestamp" column
    final_df_with_watermark = final_df.withWatermark("timestamp", "1 minute")

    # Define the output path for the CSV file
    output_path = "/home/abdelhakabdelhak/kafka/project/files/airports_stats.csv"

    """extracted_df = extracted_df.withColumn(
    "value",
    F.to_json(
        F.struct(
            "max_speed", "min_speed", "max_flag", "min_flag", "max_airport_dep",
            "min_airport_dep", "max_airport_arr", "min_airport_arr", "max_altitude",
            "min_altitude", "max_vt_speed", "min_vt_speed", "max_status", "min_status"
        )
      )
    )"""    

    # Write the extracted DataFrame to the console
    query = final_df.writeStream.format("console").outputMode("complete").start()

    """query = extracted_df_2.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094").option("topic", "schedules_stats").start()"""

    # Write the data to a single CSV file using foreachBatch
    """query = final_df.select('dep_iata', 'num_flights', 'num_flights_scheduled', 'num_flights_cancelled', 'num_flights_active', 'num_flights_landed', 'closest_flight', 'furthest_flight') \
        .writeStream \
        .foreachBatch(lambda df, epochId: df.withColumn('closest_flight', df['closest_flight'].cast('string'))
                                        .withColumn('furthest_flight', df['furthest_flight'].cast('string'))
                                        .toPandas().to_csv(output_path, mode='a', header=epochId == 0, date_format='yyyy-MM-dd HH:mm:ss')) \
        .start()"""

    # Output format for saving CSV files
    output_format = "csv"

    # Configure the writeStream to save the data to CSV files
    """query = final_df_with_watermark.writeStream \
        .format(output_format) \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", "/home/abdelhakabdelhak/kafka/project/checkpoint") \
        .trigger(processingTime="1 minute").start()"""

    # Wait for the query to terminate
    query.awaitTermination()