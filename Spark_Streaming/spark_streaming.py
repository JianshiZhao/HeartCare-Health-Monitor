import sys
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pyspark.streaming.kafka
import json
from datetime import datetime
from pyspark.sql import SQLContext, Row

# set up connection with Kafka
sc = SparkContext(appName="HealthMonitor")
ssc=StreamingContext(sc, 2)
zkQuorum, topic = ['localhost:2181','sensor']
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

# transform the json messages to a python dictionay
lines = kvs.map(lambda x:json.loads(x[1]))

# map the heart rate measurement to (hr,1) pair
lines_map = lines.map(lambda x: (x["id"],(int(x["hr"]),1)))

# define a function to get the sqlcontext, so not to create it repeatly
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']
    
# define the map reduce function to calculate the average over the time window 
def process(rdd):
    if not rdd.isEmpty():
        now_time = datetime.now()
        sqlContext = getSqlContextInstance(rdd.context)
        # first find the total hr and message count
        rdd_agg = rdd.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
        # the calculate the average using (total hr)/count
        rdd_avg = rdd_agg.map(lambda x:(x[0],round(float(x[1][0])/x[1][1])))
        # map the result to a Row object, including a timestamp
        rowRdd = rdd_avg.map(lambda x: Row(uid=x[0], time=now_time, avg=x[1]))
        # create a dataframe from Rows
        avgDataFrame = sqlContext.createDataFrame(rowRdd)
        # write the dataframe to Cassandra using the connector
        avgDataFrame.write.format("org.apache.spark.sql.cassandra")\
            .options(table="sstream",keyspace="playground")\
            .save(mode="append")
    else:
        pass

if __name__ == "__main__":  
    # calculate the average for each RDD
    lines_map.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
