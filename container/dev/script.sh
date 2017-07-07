#!/bin/sh
echo "auto.create.topics.enable=true" >> $KAFKA_HOME/config/server.properties
echo "advertised.host.name=`awk 'END{print $1}' /etc/hosts`" >> $KAFKA_HOME/config/server.properties
supervisord -n
