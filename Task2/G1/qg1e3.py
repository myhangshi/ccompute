#!/usr/bin/env python 

from cassandra.cluster import Cluster

def get_result(sess, top=10):
    wday = ("monday  ", "tuesday ", "wednesday", "thursday", "friday  ", "saturday", "sunday  ")
    query = "select * from test.g1e3" 
    #print(query)
    result = sess.execute(query)

    for val in sorted(result, key=lambda r: r.delay)[:top]: 
	    print "\t".join((wday[val.weekday-1], str(val.delay), \
                            str(val.count)))

cluster = Cluster()
session = cluster.connect()

print("Weekday\t\tAvgDelay\tCount ")
get_result(session, top=10)

