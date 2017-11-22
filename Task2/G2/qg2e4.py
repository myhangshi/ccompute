#!/usr/bin/env python 

from cassandra.cluster import Cluster

def get_result(sess, city1, city2):
    query = "select * from test.g2e4s where origin = '%s' and dest = '%s'; " \
		 % (city1, city2) 
    #print(query)
    result = sess.execute(query)

    for val in result: 
	    print val.origin, "--->",  val.dest, ": %.2f" % val.delay

cluster = Cluster()
session = cluster.connect()

for city1, city2 in (("LGA", "BOS"), ("BOS", "LGA"), ("OKC", "DFW"), \
			("MSP", "ATL")):	
    print("\n\n\nThe results for %s %s are " % (city1, city2))
    get_result(session, city1, city2)

