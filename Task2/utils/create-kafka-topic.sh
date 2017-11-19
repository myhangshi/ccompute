#!/bin/bash

ARGS=1 
export KAFKA_HOME=/home/aurora/NJ/kafka

if [ $# -ne $ARGS ];  then 
	echo "need at least one parameters"
	exit 
fi 

# Create Kafka Topic
#kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
#$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper dnjplatbuild02:2181 --replication-factor 3 --partitions 1 --topic cccapstone-group1-1
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $1  

# Confirm the Topic
#kafka-topics.sh --list --zookeeper localhost:2181

# Produce message
#kafka-console-producer.sh --broker-list localhost:9092 --topic test

# Comsume message
#kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning

# Input file to Kafka
# http://grokbase.com/t/kafka/users/157b71babg/kafka-producer-input-file
#kafka-console-produce.sh --broker-list localhost:9092 --topic my_topic --new-producer < my_file.txt


