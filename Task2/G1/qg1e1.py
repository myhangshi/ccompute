#!/usr/bin/env python 

from cassandra.cluster import Cluster

def get_result(sess, top=10):
    query = "select * from test.g1e1" 
    #print(query)
    result = sess.execute(query)

    for val in sorted(result, key=lambda r: -r.count)[:top]: 
	    print val.airport, val.count

cluster = Cluster()
session = cluster.connect()

print("\n\n\nThe results for %s are ")
get_result(session, top=10)

