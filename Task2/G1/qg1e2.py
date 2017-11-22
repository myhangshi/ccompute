#!/usr/bin/env python 

from cassandra.cluster import Cluster

def get_result(sess, top=10):
    query = "select * from test.g1e2" 
    #print(query)
    result = sess.execute(query)

    for val in sorted(result, key=lambda r: r.delay)[:top]: 
	    print "\t".join((val.carrier, str(val.delay), \
                            str(val.count))) 

cluster = Cluster()
session = cluster.connect()

print("Carrier\t\tAvgDelay\tCount ")
get_result(session, top=10)

