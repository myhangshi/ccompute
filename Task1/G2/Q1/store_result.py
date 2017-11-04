#!/usr/bin/env python 

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark import SQLContext
import uuid 

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#spark.read\
#    .format("org.apache.spark.sql.cassandra")\
#    .options(table="kv", keyspace="test")\
#    .load().show()

# write something 
#test_data = spark.read.csv("f.csv", header=False) 
#use local file "file://<file path>"
test_data = spark.sparkContext.textFile("result.csv").map(\
                    lambda l: l.strip().split("\t"))
#.map(\
#                    lambda p: [str(uuid.uuid1())] + p) 
#test_df = test_data.toDF(['uuid', 'origin','carrier', 'flight', 'performance']) 
test_df = test_data.toDF(['origin','carrier', 'airline', 'performance']) 


#kk = test_df.collect()
#print(kk)

#insert into cassandra 
test_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="g2q1", keyspace="test")\
    .save()



exit() 
