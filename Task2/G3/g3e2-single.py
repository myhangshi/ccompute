from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

import sys
import time
import datetime 
import signal 

from flight import Flight 

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def print_rdd(rdd): 
    print('==========XYZ S===================')
   
    # Get the singleton instance of SQLContext
    if rdd.isEmpty(): 
        return 

    #route, delay, details
    schema = StructType([
        StructField("route", StringType(), True), 
        StructField("delay", FloatType(), True), 
        StructField("f1", StringType(), True), 
        StructField("f2", StringType(), True),         
        StructField("details", StringType(), True)   
        ])
    
    test_df = getSqlContextInstance(rdd.context).createDataFrame(rdd, schema);  
     
    test_df.show() 

    #insert into cassandra 
    test_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="g3e2", keyspace="test")\
    .save()

    print('==========XYZ E===================')
    return 


def map_xy(f): 
    fdate = datetime.date(f.Year, f.Month, f.Day)
    return ((str(fdate), f.Dest), f)

def map_yz(f):
    fdate = datetime.date(f.Year, f.Month, f.Day)
    fdate -= datetime.timedelta(days=2)
    return ((str(fdate), f.Origin), f)

def process_record(record): 
    f_xy = record[1][0]
    f_yz = record[1][1]

    route = '_'.join((f_xy.Origin, f_xy.Dest, f_yz.Dest, record[0][0]))
    #delay = f_xy.DepDelay + f_xy.ArrDelay + f_yz.DepDelay + f_yz.ArrDelay
    delay =  f_xy.ArrDelay + f_yz.ArrDelay
    details = "%s;%s" % (f_xy, f_yz)

    #print("handling %s" % route)
    return (route, delay, f_xy.FlightNum, f_yz.FlightNum, details)

config = SparkConf()
config.set("spark.streaming.stopGracefullyOnShutdown", "true") 
    
filtered = None 
ssc = None 

config.set('spark.streaming.stopGracefullyOnShutdown', True)

sc = SparkContext(appName='g1ex2', conf=config)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
ssc.checkpoint('file:///tmp/g1ex2')

zkQuorum, topic = sys.argv[1:]
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x: x[1])

# initial data set 
ff = lines.map(lambda line: line.split(","))\
        		.map(lambda f: Flight(f))


def filter_record(f): 
    k, v = f 
   
    if k[0]=='2008-04-03' and k[1] in ('BOS', 'ATL'): 
        return True 

    if k[0]=='2008-09-07' and k[1] in ('PHX', 'JFK'): 
        return True 

    if k[0]=='2008-01-24' and k[1] in ('DFW', 'STL'): 
        return True 

    if k[0]=='2008-05-16' and k[1] in ('LAX', 'MIA'): 
        return True 

    return False 
    

f_xy = ff.map(map_xy).filter(lambda (k,v): v.CRSDepTime < '1200').filter(filter_record)
f_yz = ff.map(map_yz).filter(lambda (k,v): v.CRSDepTime > '1200').filter(filter_record)

f_xyz = f_xy.join(f_yz).map(process_record)
#.groupByKey()

#f_sorted = f_xyz.map(lambda (route, recs): (route, sorted(recs,  
#                     key = lambda recs: recs[0])[0]))

#f_sorted.pprint() 
f_xyz.foreachRDD(lambda rdd: print_rdd(rdd))

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

