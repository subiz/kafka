#!/bin/sh

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic $1 --partitions 8 --replication-factor 1 --bootstrap-server localhost:9092
