from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

import json
import pprint
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-submit'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'

"""
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'
"""

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Consumer_flights")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/abdelhakabdelhak/kafka/project/checkpoint")

    # Kafka broker address
    bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

    # Kafka topic name
    topic_name = 'flights'

    schema = StructType([
    StructField("response", ArrayType(
        StructType([
            StructField("flight_number", StringType()),
            StructField("speed", DoubleType()),
            StructField("lat", DoubleType()),
            StructField("lng", DoubleType()),
            StructField("alt", DoubleType()),
            StructField("dir", StringType()),
            StructField("hex", StringType()),
            StructField("squawk", StringType()),
            StructField("updated", LongType()),
            StructField("reg_number", StringType()),
            StructField("flag", StringType()),
            StructField("v_speed", IntegerType()),
            StructField("flight_icao", StringType()),
            StructField("flight_iata", StringType()),
            StructField("dep_icao", StringType()),
            StructField("dep_iata", StringType()),
            StructField("arr_icao", StringType()),
            StructField("arr_iata", StringType()),
            StructField("airline_icao", StringType()),
            StructField("airline_iata", StringType()),
            StructField("aircraft_icao", StringType()),
            StructField("status", StringType())
        ])
    ))
])

    # Define a UDF (User-Defined Function) to extract the max value from a list column
    def max_udf(lst):
        return float(max(lst))

    # Define a UDF to extract the min value from a list column
    def min_udf(lst):
        return float(min(lst))

    # Define UDF to find mode (most frequent) flag
    def most_flag_udf(flags):
        return max(set(flags), key=flags.count)
    
    # Define UDF to find mode (least frequent) flag
    def least_flag_udf(flags):
        return min(set(flags), key=flags.count)
    
    def most_airport_dep_udf(dep_iata):
        return max(set(dep_iata), key=dep_iata.count)

    def least_airport_dep_udf(dep_iata):
        return min(set(dep_iata), key=dep_iata.count)

    def most_airport_arr_udf(arr_iata):
        return max(set(arr_iata), key=arr_iata.count)

    def least_airport_arr_udf(arr_iata):
        return min(set(arr_iata), key=arr_iata.count)



    def max_altitude_udf(alt):
        return max(set(alt), key=alt.count)

    def min_altitude_udf(alt):
        return min(set(alt), key=alt.count)

    def max_vt_speed_udf(v_speed):
        return max(set(v_speed), key=v_speed.count)

    def min_vt_speed_udf(v_speed):
        return min(set(v_speed), key=v_speed.count)

    def max_status_udf(status):
        return max(set(status), key=status.count)

    def min_status_udf(status):
        return min(set(status), key=status.count)

    
    # Register the UDFs with Spark
    spark.udf.register("max_udf", max_udf)
    spark.udf.register("min_udf", min_udf)
    spark.udf.register("most_flag_udf", most_flag_udf)
    spark.udf.register("least_flag_udf", least_flag_udf)
    spark.udf.register("most_airport_dep_udf", most_airport_dep_udf)
    spark.udf.register("least_airport_dep_udf", least_airport_dep_udf)
    spark.udf.register("most_airport_arr_udf", most_airport_arr_udf)
    spark.udf.register("least_airport_arr_udf", least_airport_arr_udf)

    spark.udf.register("max_altitude_udf", max_altitude_udf)
    spark.udf.register("min_altitude_udf", min_altitude_udf)
    spark.udf.register("max_vt_speed_udf", max_vt_speed_udf)
    spark.udf.register("min_vt_speed_udf", min_vt_speed_udf)
    spark.udf.register("max_status_udf", max_status_udf)
    spark.udf.register("min_status_udf", min_status_udf)
                            
    df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", ",".join(bootstrap_servers))
            .option("subscribe", topic_name)
            .option("startingOffsets", "latest")
            .load()
    )

    #base_df = df.selectExpr("CAST(value as STRING)", "timestamp")


    # Extract fields from the JSON data
    extracted_df = (
        df.selectExpr("CAST(value as STRING)")
        .select(F.from_json("value", "array<struct<response:array<struct<flight_number:string,speed:double,lat:double,lng:double,alt:double,dir:string,hex:string,reg_number:string,flag:string,v_speed:double,squawk:string,flight_icao:string,flight_iata:string,dep_icao:string,dep_iata:string,arr_icao:string,arr_iata:string,airline_icao:string,airline_iata:string,aircraft_icao:string,updated:bigint,status:string>>>>").alias("data"))
        .selectExpr("explode(data.response) as response")
        .select(
            F.expr("response.flight_number").alias("flight_number"),
            F.expr("response.speed").alias("speed"),
            F.expr("response.lat").alias("lat"),
            F.expr("response.lng").alias("lng"),
            F.expr("response.alt").alias("alt"),
            F.expr("response.dir").alias("dir"),
            F.expr("response.hex").alias("hex"),
            F.expr("response.reg_number").alias("reg_number"),
            F.expr("response.flag").alias("flag"),
            F.expr("response.v_speed").alias("v_speed"),
            F.expr("response.squawk").alias("squawk"),
            F.expr("response.flight_icao").alias("flight_icao"),
            F.expr("response.flight_iata").alias("flight_iata"),
            F.expr("response.dep_icao").alias("dep_icao"),
            F.expr("response.dep_iata").alias("dep_iata"),
            F.expr("response.arr_icao").alias("arr_icao"),
            F.expr("response.arr_iata").alias("arr_iata"),
            F.expr("response.airline_icao").alias("airline_icao"),
            F.expr("response.airline_iata").alias("airline_iata"),
            F.expr("response.aircraft_icao").alias("aircraft_icao"),
            F.expr("response.updated").alias("updated"),
            F.expr("response.status").alias("status")
        )
    )

    # Create new columns for max speed, min speed, and max flag
    extracted_df = extracted_df.withColumn("max_speed", F.expr("max_udf(speed)"))
    extracted_df = extracted_df.withColumn("min_speed", F.expr("min_udf(speed)"))
    extracted_df = extracted_df.withColumn("max_flag", F.expr("most_flag_udf(flag)"))  
    extracted_df = extracted_df.withColumn("min_flag", F.expr("least_flag_udf(flag)"))  
    extracted_df = extracted_df.withColumn("max_airport_dep", F.expr("most_airport_dep_udf(dep_iata)"))  
    extracted_df = extracted_df.withColumn("min_airport_dep", F.expr("least_airport_dep_udf(dep_iata)"))  
    extracted_df = extracted_df.withColumn("max_airport_arr", F.expr("most_airport_arr_udf(arr_iata)"))  
    extracted_df = extracted_df.withColumn("min_airport_arr", F.expr("least_airport_arr_udf(arr_iata)"))  

    extracted_df = extracted_df.withColumn("max_altitude", F.expr("max_altitude_udf(alt)"))
    extracted_df = extracted_df.withColumn("min_altitude", F.expr("min_altitude_udf(alt)"))
    extracted_df = extracted_df.withColumn("max_vt_speed", F.expr("max_vt_speed_udf(v_speed)"))
    extracted_df = extracted_df.withColumn("min_vt_speed", F.expr("min_vt_speed_udf(v_speed)"))
    extracted_df = extracted_df.withColumn("max_status", F.expr("max_status_udf(status)"))
    extracted_df = extracted_df.withColumn("min_status", F.expr("min_status_udf(status)"))
    extracted_df = extracted_df.withColumn("timestamp", F.current_timestamp().cast("string"))

    """
    extracted_df = extracted_df.withColumn(
    "value",
    F.to_json(
        F.struct(
            "max_speed", "min_speed", "max_flag", "min_flag", "max_airport_dep",
            "min_airport_dep", "max_airport_arr", "min_airport_arr", "max_altitude",
            "min_altitude", "max_vt_speed", "min_vt_speed", "max_status", "min_status"
        )
      )
    )
    """

    extracted_df_2 = extracted_df.select('timestamp', 'max_speed', 'min_speed', 'max_flag', 'min_flag', 'max_airport_dep', 'min_airport_dep', 'max_airport_arr', 'min_airport_arr', 'max_altitude', 'min_altitude', 'max_vt_speed', 'min_vt_speed', 'max_status', 'min_status')

    output_path = "/home/abdelhakabdelhak/kafka/project/files/flights_stats.csv"

    # Write the extracted DataFrame to the console
    query = extracted_df_2.writeStream.format("console").outputMode("append").start()

    """query = extracted_df_2.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094").option("topic", "flights_stats").start()"""

    # Write the data to a single CSV file using foreachBatch
    """query = extracted_df_2.writeStream \
    .foreachBatch(lambda df, epochId: df.toPandas().to_csv(output_path, mode='a', header=epochId == 0)) \
    .start()"""

    # Wait for the query to finish
    query.awaitTermination()