#!/usr/bin/env python 

from cassandra.cluster import Cluster

def get_result(sess, city):
    query = "select * from test.g2e1s where origin = '%s'; " % city
    #print(query)
    result = sess.execute(query)

    for val in sorted(result, key=lambda r: r.delay)[:15]: 
	    print val.origin, val.airline, val.carrier, val.delay

cluster = Cluster()
session = cluster.connect()

for city in ("SRQ", "CMH", "JFK", "SEA", "BOS"):	
    print("\n\n\nThe results for %s are " % city)
    get_result(session, city)

