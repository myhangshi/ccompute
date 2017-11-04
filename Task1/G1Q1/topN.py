#!/usr/bin/env python

from operator import itemgetter
from collections import Counter

import sys

NUMBERS = 10 

current_word = None
current_count = 0
word = None
my_counter = Counter() 

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # convert count (currently a string) to int
    try:
        # parse the input we got from mapper.py
        word, count = line.split(',', 1)
        #print "count, word", word, count
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            #print '%s,%s' % (current_word, current_count)
            my_counter[current_word] += current_count 
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    #print '%s,%s' % (current_word, current_count)
    my_counter[current_word] += current_count 

for k, v in my_counter.most_common(NUMBERS): 
    print '%s,%s' % (k, v)

