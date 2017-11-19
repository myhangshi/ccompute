#!/bin/bash

ARGS=1 
export KAFKA_HOME=/home/aurora/NJ/kafka

if [ $# -ne $ARGS ];  then 
	echo "need at least one parameters"
	exit 
fi 

# Create Kafka Topic
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $1  



