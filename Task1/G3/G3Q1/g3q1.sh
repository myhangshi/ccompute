#!/bin/bash
hadoop fs -rm -r /output_T1/G3Q1
hadoop jar PopAirport.jar PopAirport -D delimiters=/T1/misc/delimiters.txt /dataset_T1/G3Q1 /output_T1/G3Q1
