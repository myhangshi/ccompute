#!/bin/bash

ARGS=2 
export KAFKA_HOME=/home/aurora/NJ/kafka

if [ $# -ne $ARGS ];  then 
	echo "need at least one parameters"
	exit 
fi 

# Produce message
cat $1 | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $2


