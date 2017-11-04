#!/usr/bin/env python


import csv
import sys 

def convert_data(csvfile, DEBUG=0): 
    #print "inside csv reader"
    reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
    index = 0
    old_num = 0 

    for row in reader:
        print row[1], index 
        if old_num != int(row[1]): 
            #print "old num ", old_num, row[1]
            old_num = int(row[1])
            index += 1

    return 0 

#print "length is ", len(sys.argv), sys.argv
data = open(sys.argv[1]) if len(sys.argv) > 1 else sys.stdin 


convert_data(data, DEBUG=1)

data.close() if len(sys.argv) > 1 else None

