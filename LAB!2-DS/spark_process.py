from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, from_json, to_json, struct, when, lit, mean, to_timestamp

import random

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Environment Data Processing") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Define schemas for AIR, EARTH, and WATER data
air_schema = StructType([
    StructField("Time", StringType(), True),
    StructField("Station", StringType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Moisture", DoubleType(), True),
    StructField("Light", DoubleType(), True),
    StructField("Total_Rainfall", DoubleType(), True),
    StructField("Rainfall", DoubleType(), True),
    StructField("Wind_Direction", IntegerType(), True),
    StructField("PM2.5", DoubleType(), True),
    StructField("PM10", DoubleType(), True),
    StructField("CO", DoubleType(), True),
    StructField("NOx", DoubleType(), True),
    StructField("SO2", DoubleType(), True)
])

earth_schema = StructType([
    StructField("Time", StringType(), True),
    StructField("Station", StringType(), True),
    StructField("Moisture", DoubleType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Salinity", DoubleType(), True),
    StructField("pH", DoubleType(), True),
    StructField("Water_Root", DoubleType(), True),
    StructField("Water_Leaf", DoubleType(), True),
    StructField("Water_Level", DoubleType(), True),
    StructField("Voltage", DoubleType(), True)
])

water_schema = StructType([
    StructField("Time", StringType(), True),
    StructField("Station", StringType(), True),
    StructField("pH", DoubleType(), True),
    StructField("DO", DoubleType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Salinity", DoubleType(), True)
])

# Compute statistics
def compute_statistics(df, columns, topic):
    select_exprs = [mean(col(column)).alias(f"`{column}_mean`") for column in columns]
    return df.select(select_exprs) \
        .writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName(f"stats_table_{topic}") \
        .start()

# Read streams from Kafka
air_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:29092") \
    .option("subscribe", "AIR") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

earth_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka2:39092") \
    .option("subscribe", "EARTH") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

water_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka3:49092") \
    .option("subscribe", "WATER") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON msgs into structured data with Time column cast to Timestamp
air_data = air_stream.select(from_json(col("value").cast("string"), air_schema).alias("data")).select(col("data.*"),)

earth_data = earth_stream.select(from_json(col("value").cast("string"), earth_schema).alias("data")).select(col("data.*"),)

water_data = water_stream.select(from_json(col("value").cast("string"), water_schema).alias("data")).select(col("data.*"),)

# Debugging print parsed data
air_data.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()
    
earth_data.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()
    
water_data.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()

# Start calculating statistics for each stream
air_columns = ["Temperature", "Moisture", "Light", "Total_Rainfall", "Rainfall", "`PM2.5`", "PM10", "CO", "NOx", "SO2"]
earth_columns = ["Moisture", "Temperature", "Salinity", "pH", "Water_Root", "Water_Leaf", "Water_Level", "Voltage"]
water_columns = ["pH", "DO", "Temperature", "Salinity"]

air_stats = compute_statistics(air_data, air_columns, "air")
earth_stats = compute_statistics(earth_data, earth_columns, "earth")
water_stats = compute_statistics(water_data, water_columns, "water")

# Join streams
environment_data = air_data.alias("air").join(
        earth_data.alias("earth"),["Time"],"inner"
    ).join(
        water_data.alias("water"),["Time"],"inner"
    )
    
# Debugging print joining result
environment_data.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()

# Convert the joined data to JSON
environment_json = environment_data.select(
    to_json(
        struct(
            col("Time"),
            struct(
                col("`air`.`Station`"),
                col("`air`.`Temperature`"),
                col("`air`.`Moisture`"),
                col("Light"),
                col("Total_Rainfall"),
                col("Rainfall"),
                col("Wind_Direction"),
                col("`PM2.5`"),
                col("PM10"),
                col("CO"),
                col("NOx"),
                col("SO2")
            ).alias("Air"),
            struct(
                col("`earth`.`Station`"),
                col("`earth`.`Moisture`"),
                col("`earth`.`Temperature`"),
                col("`earth`.`Salinity`"),
                col("`earth`.`pH`"),
                col("Water_Root"),
                col("Water_Leaf"),
                col("Water_Level"),
                col("Voltage")
            ).alias("Earth"),
            struct(
                col("`water`.`Station`"),
                col("`water`.`pH`"),
                col("DO"),
                col("`water`.`Temperature`"),
                col("`water`.`Salinity`")
            ).alias("Water")
        )
    ).alias("value")
)
    
# Write final stream to HDFS
query = environment_json.writeStream \
    .format("json") \
    .option("path", "/hdfs/environment_data/") \
    .option("checkpointLocation", "/hdfs/checkpoints/environment_data/") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()
