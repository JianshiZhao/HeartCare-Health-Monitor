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

sc = SparkContext(appName="HealthMonitor")
ssc=StreamingContext(sc, 2)
zkQuorum, topic = ['localhost:2181','sensor']
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x:json.loads(x[1]))


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']
    

lines_map = lines.map(lambda x: (x["id"],(int(x["hr"]),1)))

def process(rdd):
    if not rdd.isEmpty():
        now_time = datetime.now()
        sqlContext = getSqlContextInstance(rdd.context)
        rdd_agg = rdd.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
        rdd_avg = rdd_agg.map(lambda x:(x[0],round(float(x[1][0])/x[1][1])))
        rowRdd = rdd_avg.map(lambda x: Row(uid=x[0], time=now_time, avg=x[1]))
        avgDataFrame = sqlContext.createDataFrame(rowRdd)
    
        avgDataFrame.write.format("org.apache.spark.sql.cassandra").options(table="sstream",keyspace="playground").save(mode="append")
    else:
        pass
    
lines_map.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
