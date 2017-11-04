#!/bin/bash

hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar \
    -mapper mapper.py \
    -reducer topN.py \
    -input  /dataset_T1/G1Q1-2 \
    -output /output_T1/G1Q1-2 \
    -file mapper.py \
    -file topN.py

