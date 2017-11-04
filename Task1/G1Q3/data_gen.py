#!/usr/bin/env python

import csv
import sys 

def convert_data(csvfile, DEBUG=0): 
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    rownum = 0 
    
    for row in reader:
    	if rownum == 0: 
    		header = row 
    		if DEBUG: 
    			print(header)
    	else: 
    		colnum = 0 
    		print  "%s,%s" % (row[4], row[36])
    		
    
    		if DEBUG: 
    			for col in row:
    				print "%d:  %s: %s" % (colnum, header[colnum], col)
    				colnum += 1
    
    	rownum += 1	
    	if DEBUG:
    		if rownum > 3: 
    			#close()
    			return 0

    return 0 

#print "length is ", len(sys.argv), sys.argv
data = open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin 

convert_data(data, DEBUG=0)

data.close() if len(sys.argv) > 1 else None


