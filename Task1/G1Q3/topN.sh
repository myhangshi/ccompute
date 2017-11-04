#!/bin/bash

#hadoop fs -rm -r /output_T1/G1Q3

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar \
    -mapper mapper.py \
    -reducer topN.py \
    -input  /dataset_T1/G1Q3 \
    -output /output_T1/G1Q3 \
    -file mapper.py \
    -file topN.py

