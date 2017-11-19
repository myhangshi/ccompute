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

#group_by_origin_dest = GROUP in BY (Origin, Dest);

#average_ontime = FOREACH group_by_origin_dest 
#                 GENERATE FLATTEN(group) AS (Origin, Dest), 
#                          AVG(in.DepDelay) AS performance_index;


#group_by_origin = GROUP average_ontime BY Origin; 

#top_ten_airports = FOREACH group_by_origin {
#   sorted_airports = ORDER average_ontime BY performance_index ASC;
#   top_airports = LIMIT sorted_airports 10;
#   GENERATE FLATTEN(top_airports);


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

    schema = StructType([
        StructField("origin", StringType(), True),
        StructField("delay", FloatType(), True), 
        StructField("dest", StringType(), True)
        ])
    
    test_df = getSqlContextInstance(rdd.context).createDataFrame(rdd, schema);  
    
    test_df.show() 

    #insert into cassandra 
    test_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('overwrite')\
    .options(table="g2e2", keyspace="test")\
    .save()

    print('==========XYZ E===================')
    return 

config.set('spark.streaming.stopGracefullyOnShutdown', True)

#sc = SparkContext(appName='g1ex1', conf=config, pyFiles=['flight.py'])
signal.signal(signal.SIGINT, close_handler)

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
                .map(lambda f: ((f.Origin, f.Dest), (f.DepDelay, 1)))\
        		.updateStateByKey(updateFunction)

filtered = f1.map(lambda (x, y): (x[0], y[2], x[1]))

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
    

#spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2    ./g2e1.py localhost:2181 g2e1 
#spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2    ./g2e1.py localhost:2181 g2e1  | tee t4.log 

