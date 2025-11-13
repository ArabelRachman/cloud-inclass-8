# Read the main documentation here https://spark.apache.org/docs/latest/streaming/index.html
#
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, col, count, current_timestamp, sum as spark_sum, round as spark_round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("AirlineCancellationPercentage_Task3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")



flightSchema = StructType() \
    .add("YEAR", IntegerType(), True) \
    .add("MONTH", IntegerType(), True) \
    .add("DAY", IntegerType(), True) \
    .add("DAY_OF_WEEK", IntegerType(), True) \
    .add("AIRLINE", StringType(), True) \
    .add("FLIGHT_NUMBER", IntegerType(), True) \
    .add("TAIL_NUMBER", StringType(), True) \
    .add("ORIGIN_AIRPORT", StringType(), True) \
    .add("DESTINATION_AIRPORT", StringType(), True) \
    .add("SCHEDULED_DEPARTURE", IntegerType(), True) \
    .add("DEPARTURE_TIME", IntegerType(), True) \
    .add("DEPARTURE_DELAY", IntegerType(), True) \
    .add("TAXI_OUT", IntegerType(), True) \
    .add("WHEELS_OFF", IntegerType(), True) \
    .add("SCHEDULED_TIME", IntegerType(), True) \
    .add("ELAPSED_TIME", IntegerType(), True) \
    .add("AIR_TIME", IntegerType(), True) \
    .add("DISTANCE", IntegerType(), True) \
    .add("WHEELS_ON", IntegerType(), True) \
    .add("TAXI_IN", IntegerType(), True) \
    .add("SCHEDULED_ARRIVAL", IntegerType(), True) \
    .add("ARRIVAL_TIME", IntegerType(), True) \
    .add("ARRIVAL_DELAY", IntegerType(), True) \
    .add("DIVERTED", IntegerType(), True) \
    .add("CANCELLED", IntegerType(), True) \
    .add("CANCELLATION_REASON", StringType(), True) \
    .add("AIR_SYSTEM_DELAY", IntegerType(), True) \
    .add("SECURITY_DELAY", IntegerType(), True) \
    .add("AIRLINE_DELAY", IntegerType(), True) \
    .add("LATE_AIRCRAFT_DELAY", IntegerType(), True) \
    .add("WEATHER_DELAY", IntegerType(), True)


# Download the file from here 
# https://www.cs.utexas.edu/~kiat/datasets/flights.csv.bz2
# mkdir flights 
# move the file into that folder
# unzip it. 
# bunzip2 flights.csv.bz2
# you should have inside flights folder a file with name flights.csv 
# 565M 

flights = spark \
    .readStream \
    .option("sep", ",") \
    .schema(flightSchema) \
    .csv("flights")

flights.printSchema()


# Add timestamp column
lines = flights.withColumn("timestamp", current_timestamp())

# Task 1: Which Airline has the most flights in the last 3 minutes? Update every 5 seconds

# windowed_counts = lines \
#     .select("timestamp", "AIRLINE")\
#     .groupBy(
#         window(col("timestamp"), "3 minutes", "5 seconds"),
#         col("AIRLINE")
#     ).count() \
#     .orderBy(col("count").desc())

# Task 2: Which Airline has the most cancellations in the last 3 minutes? Update every 5 seconds

# cancelled_flights = lines \
#     .filter(col("CANCELLED") == 1) \
#     .select("timestamp", "AIRLINE")
# windowed_cancellation_counts = cancelled_flights \
#     .groupBy(
#         window(col("timestamp"), "3 minutes", "5 seconds"),
#         col("AIRLINE")
#     ).count() \
#     .orderBy(col("count").desc())

# Task 3: Which Airline has the highest cancellation percentage in the last 3 minutes? Update every 5 seconds
# (cancelled flights / total flights) * 100 
windowed_airline_stats = lines \
    .select("timestamp", "AIRLINE", "CANCELLED") \
    .groupBy(
        window(col("timestamp"), "3 minutes", "5 seconds"),
        col("AIRLINE")
    ).agg(
        count("*").alias("total_flights"),
        spark_sum(col("CANCELLED")).alias("cancelled_flights")
    ).withColumn(
        "cancellation_percentage", 
        spark_round((col("cancelled_flights") / col("total_flights")) * 100, 2)
    ).orderBy(col("cancellation_percentage").desc())

# Output to console - show the last 10 values
query = windowed_airline_stats.writeStream \
    .trigger(processingTime='5 seconds')\
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 100) \
    .start()

query.awaitTermination()