#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    try: #sometimes bad data can cause errors use this how you like to deal with lint and bad data
         
        # remove leading and trailing whitespace
        line = line.strip()
        # split the line into words
        (origin, dest) = line.split(',')
        #print " old new ", origin, dest 
        
        print '%s,1' % (origin)
        print '%s,1' % (dest)
    except: #errors are going to make your job fail which you may or may not want
        continue


