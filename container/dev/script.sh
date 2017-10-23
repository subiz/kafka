#!/bin/sh

sed -e '62d;66d' $KAFKA_HOME/config/server.properties
echo "auto.create.topics.enable=true" >> $KAFKA_HOME/config/server.properties
echo "advertised.host.name=`awk 'END{print $1}' /etc/hosts`" >> $KAFKA_HOME/config/server.properties
echo "num.partitions=10" >> $KAFKA_HOME/config/server.properties
echo "delete.topic.enable=true" >> $KAFKA_HOME/config/server.properties


echo "
k() {
	echo \"list, count $1, consume $1, cleartopic $1, desc $1, alter $1, $2\"
}

list() {
	/opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh --zookeeper localhost:2181 --list
}

count() {
	/opt/kafka_2.11-0.10.1.0/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic \"\$1\" --time -1 |cut -c 22- | awk '{total = total + \$1}END{print total}'
}

consume() {
	/opt/kafka_2.11-0.10.1.0/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic \"\$1\" --from-beginning
}

cleartopic() {
	/opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic \"\$1\"
}

desc() {
	./kafka-topics.sh --describe --zookeeper localhost:2181 --topic \"\$1\" | more
}

alter() {
	/opt/kafka_2.11-0.10.1.0/bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic \"\$1\" --partitions \"\$2\"
}
" >> ~/.bashrc
supervisord -n
