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
test_data = spark.sparkContext.textFile("file:////home/aurora/NJ/aurora/Task1/G3/G3Q2/q32.csv").map(\
                    lambda l: l.strip().split("\t"))
#.map(\
#                    lambda p: [str(uuid.uuid1())] + p) 
#test_df = test_data.toDF(['uuid', 'origin','carrier', 'flight', 'performance']) 
test_df = test_data.toDF(['fdate','forigin', 'fto', 'sto', 'carrier', 
						'depart', 'flight', 'scarrier', 'arrival', 'number']) 


#insert into cassandra 
test_df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="g3q2", keyspace="test")\
    .save()



exit() 
