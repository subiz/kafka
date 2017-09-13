#!/bin/sh

sed -e '62d;66d' $KAFKA_HOME/config/server.properties
echo "auto.create.topics.enable=true" >> $KAFKA_HOME/config/server.properties
echo "advertised.host.name=`awk 'END{print $1}' /etc/hosts`" >> $KAFKA_HOME/config/server.properties
echo "num.partitions=10" >> $KAFKA_HOME/config/server.properties
echo "delete.topic.enable=true" >> $KAFKA_HOME/config/server.properties
supervisord -n
