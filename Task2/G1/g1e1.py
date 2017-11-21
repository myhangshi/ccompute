from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

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

def print_rdd(rdd):
    print('==========XYZ S===================')
    airports = rdd.takeOrdered(10, key = lambda x: -x[1])
    for airport in airports:
        print("%s,%d" % (airport[0], airport[1]))
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

filtered = lines.map(lambda line: line.split("\t"))\
        		.flatMap(lambda word: [(word[3], 1), (word[4], 1)] if len(word) > 4 else [] )\
        		.reduceByKey(lambda a, b: a+b)\
                .updateStateByKey(updateFunction)\
                .transform(lambda rdd: rdd.sortBy(lambda (word, count): -count))

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
    