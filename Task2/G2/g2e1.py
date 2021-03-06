from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

import sys
import time
import signal 

import itertools 
import cassandra 

from cassandra.cluster import Cluster
from cassandra.query import named_tuple_factory 
from flight import Flight 
from itertools import islice, chain


config = SparkConf()
config.set("spark.streaming.stopGracefullyOnShutdown", "true") 
	
filtered = None 
ssc = None 

def grouper_it(n, iterable):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


def save_data_to_DB(iter): 
    cluster = Cluster() 
    session = cluster.connect() 

    for b in grouper_it(50, iter):
        print('==========XYZ S===================')
    
        data = ''.join(["""
    INSERT INTO test.g2e1 (origin, airline, carrier, delay)
    VALUES ('%s', '%s', '%s', %s); \n
    """ % (r[0], r[1], r[2], str(r[3])) for r in b ])  
        print(data)
        session.execute_async("BEGIN BATCH\n" + data + " APPLY BATCH")
        print('==========XYZ E===================')
    
    session.shutdown() 
    
    return 


config.set('spark.streaming.stopGracefullyOnShutdown', True)
config.set('spark.executor.memory', "8G")
config.set('spark.driver.memory', "8G")
   
sc = SparkContext(appName='g1ex2', conf=config)
sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 10)
ssc.checkpoint('file:///tmp/g1ex2')

zkQuorum, topic = sys.argv[1:]
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x: x[1])

def updateFunction(newValues, runningCount):
    values, counter, avg_delay = runningCount or (0., 0, 0.)
    for val in newValues: 
        values += val[0]
        counter += val[1]

    return (values, counter, values/counter) 

f1 = lines.map(lambda line: line.split(","))\
        		.map(lambda f: Flight(f))\
                .map(lambda f: ((f.Origin, f.Carrier, f.Airline), (f.DepDelay, 1)))\
        		.updateStateByKey(updateFunction)

filtered = f1.map(lambda (x, y): (x[0], x[1], x[2], y[2]))

#filtered.foreachRDD(lambda rdd: print_rdd(rdd))
#filtered.pprint() 
filtered.foreachRDD(lambda rdd: rdd.foreachPartition(save_data_to_DB))


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
    

#spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2    ./g2e1.py localhost:2181 g2e1 
#spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2    ./g2e1.py localhost:2181 g2e1  | tee t4.log 

