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

#group_by_origin_dest_airline = GROUP in BY (Origin, Dest, AirlineID);


#average_ontime = FOREACH group_by_origin_dest_airline
#               GENERATE FLATTEN(group) AS (Origin, Dest, AirlineID),
#               AVG(in.ArrDelay) AS performance_index;

#group_by_origin_dest = GROUP average_ontime BY (Origin, Dest);

#top_ten_airlines = FOREACH group_by_origin_dest {
#   sorted_airlines = ORDER average_ontime BY performance_index ASC;
#   top_airlines = LIMIT sorted_airlines 10;
#   GENERATE FLATTEN(top_airlines);
#}

#X = FOREACH top_ten_airlines GENERATE  $0, $1, $3, $2;

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

#((f.Origin, f.Dest, f.Carrier, f.Airline), (f.ArrDelay, 1)))

    schema = StructType([
        StructField("origin", StringType(), True),
        StructField("dest", StringType(), True), 
        StructField("airline", StringType(), True),       
        StructField("delay", FloatType(), True), 
        ])
    
    test_df = getSqlContextInstance(rdd.context).createDataFrame(rdd, schema);  
    #"origin:string, delay:float, carrier:string,  ariline:string");  

    test_df.show() 

    #insert into cassandra 
    test_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('overwrite')\
    .options(table="g2e3", keyspace="test")\
    .save()

    print('==========XYZ E===================')
    return 

#sc = SparkContext(appName='g1ex1', conf=config, pyFiles=['flight.py'])
config.set('spark.streaming.stopGracefullyOnShutdown', True)

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
                .map(lambda f: ((f.Origin, f.Dest, f.Carrier, f.Carrier), (f.ArrDelay, 1)))\
        		.updateStateByKey(updateFunction)

filtered = f1.map(lambda (x, y): (x[0], x[1], x[3], y[2]))

filtered.foreachRDD(lambda rdd: print_rdd(rdd))
#filtered.pprint() 

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

