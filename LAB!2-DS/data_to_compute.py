from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit, avg, max, min
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Centralized Computing for Mango Growth Evaluation") \
    .getOrCreate()

# HDFS Path
input_path = "hdfs://namenode:9000/hdfs/environment_data/*.json"
output_path = "hdfs://namenode:9000/hdfs/mango_data_centralized.txt"

# Define Schema for Nested JSON
schema = StructType([
    StructField("Air", StructType([
        StructField("Temperature", DoubleType()),
        StructField("Moisture", DoubleType())
    ])),
    StructField("Earth", StructType([
        StructField("pH", DoubleType())
    ]))
])

# Define Ideal Conditions
ideal_conditions = {
    "Air_Temperature": (23.0, 28.0),
    "Air_Moisture": (60.0, 80.0),
    "Earth_pH": (5.5, 7.0)
}

def process_data():
    # Load JSON Data and collect to driver
    df = spark.read.json(input_path)
    
    # Parse the "value" field
    parsed_df = df.withColumn("parsed_value", from_json(col("value"), schema))
    
    # Extract the parsed fields
    data_df = parsed_df.select(
        col("parsed_value.Air.Temperature").alias("Air_Temperature"),
        col("parsed_value.Air.Moisture").alias("Air_Moisture"),
        col("parsed_value.Earth.pH").alias("Earth_pH")
    )
    
    # Collect all data to driver node
    collected_data = data_df.collect()
    
    # Convert collected data to a new local DataFrame
    local_df = spark.createDataFrame(collected_data)
    
    # Calculate Conditions Out of Ideal
    out_of_ideal_df = local_df.withColumn(
        "Air_Temperature_Out",
        when((col("Air_Temperature") < lit(ideal_conditions["Air_Temperature"][0])) |
             (col("Air_Temperature") > lit(ideal_conditions["Air_Temperature"][1])), 1).otherwise(0)
    ).withColumn(
        "Air_Moisture_Out",
        when((col("Air_Moisture") < lit(ideal_conditions["Air_Moisture"][0])) |
             (col("Air_Moisture") > lit(ideal_conditions["Air_Moisture"][1])), 1).otherwise(0)
    ).withColumn(
        "Earth_pH_Out",
        when((col("Earth_pH") < lit(ideal_conditions["Earth_pH"][0])) |
             (col("Earth_pH") > lit(ideal_conditions["Earth_pH"][1])), 1).otherwise(0)
    )
    
    # Calculate time out of ideal conditions locally
    out_of_ideal_count = out_of_ideal_df.filter(
        (col("Air_Temperature_Out") + col("Air_Moisture_Out") + col("Earth_pH_Out")) >= 1
    ).count()
    
    # Convert from minutes to hours, minutes
    hours = out_of_ideal_count // 60
    minutes = out_of_ideal_count % 60
    
    # Calculate statistics locally
    stats = local_df.agg(
        max("Air_Temperature").alias("Max_Air_Temperature"),
        min("Air_Temperature").alias("Min_Air_Temperature"),
        max("Air_Moisture").alias("Max_Air_Moisture"),
        min("Air_Moisture").alias("Min_Air_Moisture"),
        max("Earth_pH").alias("Max_Earth_pH"),
        min("Earth_pH").alias("Min_Earth_pH")
    ).collect()[0]
    
    # Format results
    results = (
        f"Average Time Out of Ideal: {hours} hours {minutes} minutes\n"
        f"Max Air Temperature: {stats['Max_Air_Temperature']}°C\n"
        f"Min Air Temperature: {stats['Min_Air_Temperature']}°C\n"
        f"Max Air Moisture: {stats['Max_Air_Moisture']}%\n"
        f"Min Air Moisture: {stats['Min_Air_Moisture']}%\n"
        f"Max Earth pH: {stats['Max_Earth_pH']}\n"
        f"Min Earth pH: {stats['Min_Earth_pH']}\n"
    )
    
    # Write results to HDFS as a single text file
    spark.sparkContext.parallelize([results]).saveAsTextFile(output_path)

# Process the data
process_data()

# Stop SparkSession
spark.stop()