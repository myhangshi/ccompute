#!/usr/bin/env python 

from cassandra.cluster import Cluster

def get_result(sess, route):
    query = "select route, delay, f1, f2, details from test.g3e2 where route = '%s' group by delay limit 1; " % route 
    #print(query)
    result = sess.execute(query)

    for val in result: 
        print val.route, val.delay, val.f1, val.f2, val.details 

cluster = Cluster()
session = cluster.connect()

for route in ('DFW_STL_ORD_2008-01-24', \
		'BOS_ATL_LAX_2008-04-03', \
		'LAX_MIA_LAX_2008-05-16', \
		'PHX_JFK_MSP_2008-09-07'):	
    print("\n\n\nThe results for %s are " % route)
    get_result(session, route)

