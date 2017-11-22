from pyspark import SparkContext
from pyspark import SparkConf

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *

import sys
import time
import signal 

config = SparkConf()
config.set("spark.streaming.stopGracefullyOnShutdown", "true") 
	
filtered = None 
ssc = None 

def close_handler(signal, frame): 
	print('Closing down, print out result ')
	try: 
		if filtered: 
			filtered.foreachRDD(lambda rdd: print_rdd(rdd))
		if ssc: 
			ssc.stop(true, true)
	except: 
		pass 	
	sys.exit(0)	 

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def print_rdd(rdd):
    print('==========XYZ S===================')
        # Get the singleton instance of SQLContext
    if rdd.isEmpty(): 
        return 
        
    #airports = rdd.takeOrdered(10, key = lambda x: -x[1])
    #for airport in airports:
    #    print("%s,%d" % (airport[0], airport[1]))


    schema = StructType([
        StructField("airport", StringType(), True),
        StructField("count", IntegerType(), True)
        ])
    
    test_df = getSqlContextInstance(rdd.context).createDataFrame(rdd, schema);  
    #"origin:string, delay:float, carrier:string,  ariline:string");  

    test_df.show() 

    #insert into cassandra 
    test_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="g1e1", keyspace="test")\
    .save()

    print('==========XYZ E===================')

config.set('spark.streaming.stopGracefullyOnShutdown', True)

#sc = SparkContext(appName='g1ex1', conf=config, pyFiles=['flight.py'])
signal.signal(signal.SIGINT, close_handler)


sc = SparkContext(appName='g1ex1', conf=config)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
ssc.checkpoint('file:///tmp/g1ex1')

#lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

zkQuorum, topic = sys.argv[1:]
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x: x[1])

def updateFunction(newValues, runningCount):
        return sum(newValues, runningCount or 0)

filtered = lines.map(lambda line: line.split(","))\
        		.flatMap(lambda word: [(word[8], 1), (word[9], 1)] )\
        		.reduceByKey(lambda a, b: a+b)\
                .updateStateByKey(updateFunction)

#                .transform(lambda rdd: rdd.sortBy(lambda (word, count): -count))

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
    