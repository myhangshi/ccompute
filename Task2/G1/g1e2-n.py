from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row

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

    sqlContext = getSqlContextInstance(rdd.context)
   
    dataFrame = sqlContext.createDataFrame(rdd,  
                    "carrier:string, delay:float, total:int")
    dataFrame.show() 
    dataFrame.registerTempTable("carrier_delays")
    # Do word count on table using SQL and print it
    carrier_delays_df = \
                sqlContext.sql("SELECT carrier, delay/total AS avg_delay FROM \
                    carrier_delays  ORDER BY avg_delay ASC LIMIT 10")
    carrier_delays_df.show()

    #airlines = rdd.takeOrdered(10, key = lambda x: -x[1][0]/airline[1][1])
    airlines = rdd.takeOrdered(10, key = lambda (x,y, z): float(z)/float(y))

    for (x, y, z) in airlines:
        print("%s, %f,%d" % (x, y/z, z))
    print('==========XYZ E===================')


config.set('spark.streaming.stopGracefullyOnShutdown', True)

#sc = SparkContext(appName='g1ex1', conf=config, pyFiles=['flight.py'])
signal.signal(signal.SIGINT, close_handler)


sc = SparkContext(appName='g1ex2', conf=config)
ssc = StreamingContext(sc, 10)
ssc.checkpoint('file:///tmp/g1ex2')

zkQuorum, topic = sys.argv[1:]
kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
lines = kvs.map(lambda x: x[1])

def updateFunction(newValues, runningCount):
    if runningCount is None: 
        runningCount = (0., 0)
    
    values, counter = runningCount 
    for val in newValues: 
        values += val[0]
        counter += val[1]

    return (values, counter) 

filtered = lines.map(lambda line: line.split("\t"))\
        		.map(lambda word: (word[0]+" " + word[1], (float(word[7]), 1)) )\
        		.updateStateByKey(updateFunction)\
                .map(lambda (x, y): (x, float(y[0]), int(y[1])) )


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
    