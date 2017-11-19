#!/usr/bin/env python

import csv
import sys 

def convert_data(csvfile, DEBUG=0): 
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    rownum = 0 
    
    included_cols = [0, 2, 3, 4, 5, 7, 8, 10, 11, 17, 23, 25, 34, 36]
    for r in reader:
    	if rownum == 0: 
    		header = r 
    		if DEBUG: 
    			print(header)
    	else: 
    		#print  "%s,%s" % (row[13], row[20])
            content = list(r[i] for i in included_cols)
            print(','.join(content))
    
    
    	rownum += 1	
    	if DEBUG:
    		if rownum > 3: 
    			#close()
    			return 0

    return 0 

#print "length is ", len(sys.argv), sys.argv
data = open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin 

convert_data(data)

data.close() if len(sys.argv) > 1 else None


