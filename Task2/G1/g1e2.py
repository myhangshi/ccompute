from pyspark import SparkContext
from pyspark import SparkConf

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

import sys
import time
import signal 

from flight import Flight 

config = SparkConf()
config.set("spark.streaming.stopGracefullyOnShutdown", "true") 
	
filtered = None 
ssc = None 


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def print_rdd(rdd):
    print('==========XYZ S===================')
        # Get the singleton instance of SQLContext
    if rdd.isEmpty(): 
        return 


    schema = StructType([
        StructField("carrier", StringType(), True),
        StructField("delay", FloatType(), True),
        StructField("count", IntegerType(), True)
        ])
    
    test_df = getSqlContextInstance(rdd.context).createDataFrame(rdd, schema);  
    
    test_df.show() 

    #insert into cassandra 
    test_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="g1e2", keyspace="test")\
    .save()

    print('==========XYZ E===================')


config.set('spark.streaming.stopGracefullyOnShutdown', True)

sc = SparkContext(appName='g1ex2', conf=config)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
ssc.checkpoint('file:///tmp/g1ex2')

zkQuorum, topic = sys.argv[1:]
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x: x[1])

def updateFunction(newValues, runningCount):
    values, counter = runningCount or (0., 0) 
    for val in newValues: 
        values += val[0]
        counter += val[1]
    return (values, counter) 

filtered = lines.map(lambda line: line.split(","))\
                .map(lambda f: Flight(f))\
                .map(lambda f: (f.Airline+"_" + f.Carrier, (f.ArrDelay, 1)) )\
                .updateStateByKey(updateFunction)\
                .map(lambda (x, y): (x, y[0]/y[1], y[1]) )

filtered.foreachRDD(lambda rdd: print_rdd(rdd))

# start streaming process
ssc.start()

try:
    ssc.awaitTermination()
except:
    pass

try:
    time.sleep(10)
except:
    pass
    
