#!/usr/bin/env python

from collections import defaultdict
import sys

current_word = None
current_count = 0
current_total = 0.0

word = None
my_counter = defaultdict(int) 
my_value = defaultdict(float)

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # convert count (currently a string) to int
    try:
        # parse the input we got from mapper.py
        word, count, value = line.split(',')       
        count = int(count)
        value = float(value)
        #print "word, count, value ", word, count, value
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
        current_value += value 
    else:
        if current_word:
            # write result to STDOUT
            #print '%s,%s' % (current_word, current_count)
            my_counter[current_word] += current_count
            my_value[current_word] += current_value 
            #print "1 fill in  count, value ", current_count, current_value
        current_count = count
        current_value = value 
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    #print '%s,%s' % (current_word, current_count)
    my_counter[current_word] += current_count 
    my_value[current_word] += current_value 
    #print "2 fill in  count, value ", current_count, current_value

for k, v in my_value.items(): 
    my_value[k] = my_value[k] / my_counter[k]

sorted_values = sorted(my_value.iteritems() , key= lambda (k, v):(v, k))

for v in sorted_values: 
    print("%s,%s"% (v[0],  v[1]))

