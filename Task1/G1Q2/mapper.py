#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    try: #sometimes bad data can cause errors use this how you like to deal with lint and bad data
         
        # remove leading and trailing whitespace
        line = line.strip()
        # split the line into words
        (airline, delay) = line.split(',')
        #print " old new ", airline, delay
        
        print '%s,1,%s' % (airline, delay)
        
    except: #errors are going to make your job fail which you may or may not want
        continue


