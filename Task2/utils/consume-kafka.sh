#!/bin/bash

ARGS=1 
export KAFKA_HOME=/home/aurora/NJ/kafka

if [ $# -ne $ARGS ];  then 
	echo "need at least one parameters"
	exit 
fi 

# consume message
$KAFKA_HOME/bin/kafka-console-consumer.sh --broker-list localhost:2181 --topic $1


