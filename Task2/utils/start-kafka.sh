#!/usr/bin/env bash

export KAFKA_HOME=/home/aurora/NJ/kafka

export LOG_DIR=/data/hadoop/logs
export KAFKA_LOG4J_OPTS="-Dkafka.logs.dir=$LOG_DIR $KAFKA_LOG4J_OPTS"

# Start Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &

# Create Kafka Topic
#kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
#$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper dnjplatbuild02:2181 --replication-factor 3 --partitions 1 --topic my-replicated-topic

# Confirm the Topic
#kafka-topics.sh --list --zookeeper localhost:2181

# Produce message
#kafka-console-producer.sh --broker-list localhost:9092 --topic test

# Comsume message
#kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
#kafka-console-consumer.sh --zookeeper dnjplatbuild02:2181 --topic my-replicated-topic --from-beginning

# Input file to Kafka
# http://grokbase.com/t/kafka/users/157b71babg/kafka-producer-input-file
#kafka-console-produce.sh --broker-list localhost:9092 --topic my_topic --new-producer < my_file.txt


