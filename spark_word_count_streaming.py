import sys
from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# create spark configuration
conf = SparkConf()
conf.setAppName("AirlineCancellationCount")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")
# create the Streaming Context from the above spark context with interval size 5

ssc = StreamingContext(sc, 5)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_airline_flights")
# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)

# Task 1: Which Airline has the most flights in the last 3 minutes
# 
# airlines = dataStream.window(180, 5).map(lambda line: line.split(',')[4] if len(line.split(',')) > 4 else None).filter(lambda x: x is not None)
# airline_count = airlines.map(lambda x: (x, 1)).reduceByKey(add)

# sort by flight count descending
# result = airline_count.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# Task 2: Which Airline has the most cancellations in the last 3 minutes
cancelled_flights = dataStream.window(180, 5).map(lambda line: line.split(',') if len(line.split(',')) > 24 else None).filter(lambda x: x is not None and len(x) > 24 and x[24] == '1').map(lambda x: x[4])
airline_cancellation_count = cancelled_flights.map(lambda x: (x, 1)).reduceByKey(add)
# sort by cancellation count descending
result = airline_cancellation_count.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
# printing top 10
result.pprint(10)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

