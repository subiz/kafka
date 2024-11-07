
## Install java
```sh
sudo apt install openjdk-19-jdk
update-alternatives --config java
```

## Download kafa
Download the [latest Kafka](https://www.apache.org/dyn/closer.cgi?path=/kafka/3.3.1/kafka_2.13-3.3.1.tgz) release and extract it:
```sh
$ wget https://dlcdn.apache.org/kafka/3.3.1/kafka_2.13-3.3.1.tgz
$ tar -xzf kafka_2.13-3.3.1.tgz
$ sudo mv kafka_2.13-3.3.1.tgz /opt/
$ cd /opt/kafka_2.13-3.3.1
```

### Install kafka KRaft
Generate a Cluster UUID
```sh
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```
// Pyi9Id7fRSWlmId_KU07LQ
Format Log Directories

```sh
$ KAFKA_CLUSTER_ID=Pyi9Id7fRSWlmId_KU07LQ
$ /opt/kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c /opt/kafka/config/kraft/server.properties
```
Start the Kafka Server
`
$ bin/kafka-server-start.sh config/kraft/server.properties
`
Once the Kafka server has successfully launched, you will have a basic Kafka environment running and ready to use.

### Config kafka
Edit `/opt/kafka/config/server.properties`
Change `broker.id=0` to `broker.id=1`
Change `num.partitions=1` to `num.partitions=50`
Add `listeners=PLAINTEXT://kafka-1:9092`
Add `advertised.listeners=PLAINTEXT://kafka-1:9092`

Also, change the listeners key  `/opt/kafka/config/kraft/server.properties`

`advertised.listeners=PLAINTEXT://kafka-1:9092`
`log.dirs=/var/log/kraft-combined-logs`
`num.partitions=50`

### Create service
```sh
nano /etc/systemd/system/kafka.service
```
paste
```
[Service]
Type=simple
User=root

ExecStart=/bin/sh -c '/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties > >
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
```

Start kafka on start up
```sh
systemctl enable kafka
```

Start kafkaf

```sh
systemctl start kafka
```

### Tesing
#### Create a topic
```sh
/opt/kafka/bin/kafka-topics.sh --create --topic foo --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
```
All of Kafka's command line tools have additional options: run the kafka-topics.sh command without any arguments to display usage information. For example, it can also show you details such as the partition count of the new topic:
```sh
$ /opt/kafka/bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
Topic: quickstart-events        TopicId: NPmZHyhbR9y00wMglMH2sg PartitionCount: 1       ReplicationFactor: 1	Configs:
    Topic: quickstart-events Partition: 0    Leader: 0   Replicas: 0 Isr: 0
```
#### Write some event into the topic

```sh
$ /opt/kafka/bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
This is my first event
This is my second event
```

#### Read event
```sh
$ /opt/kafka/bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```

### List topics
```sh
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka-1:9092


/opt/kafka/bin/kafka-topics.sh --create --topic search-index-0 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic search-index-1 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic search-index-2 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic search-index-3 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092


/opt/kafka/bin/kafka-topics.sh --create --topic lead-index-0 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic lead-index-1 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic lead-index-2 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic lead-index-3 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic lead-index-4 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic lead-index-5 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic lead-index-6 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic lead-index-7 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic lead-index-8 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic lead-index-9 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic user-index-0 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-index-1 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-index-2 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-index-3 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-index-4 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-index-5 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-index-6 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-index-7 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-index-8 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-index-9 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092


/opt/kafka/bin/kafka-topics.sh --create --topic user-updated-0 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-updated-1 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-updated-2 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-updated-3 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-updated-4 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-updated-5 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-updated-6 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-updated-7 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-updated-8 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-updated-9 --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic scheduler --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic test --partitions 2 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic scheduler --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic credit-spend-log --partitions 50 --replication-factor 1 --bootstrap-server kafka-1:9092


/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-convo-0 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-convo-1 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-convo-2 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-convo-3 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-convo-4 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-convo-5 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-convo-6 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-convo-7 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092


/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-task-0 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-task-1 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-task-2 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic scheduler-task-3 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092


/opt/kafka/bin/kafka-topics.sh --create --topic ticket-updated-0 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic ticket-updated-1 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic ticket-updated-2 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic ticket-updated-3 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic ticket-updated-4 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic ticket-updated-5 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic ticket-updated-6 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic ticket-updated-7 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic integration-sync-facebook-0 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic integration-sync-facebook-1 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic integration-sync-facebook-2 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic integration-sync-facebook-3 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic integration-sync-facebook-4 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic integration-sync-facebook-5 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic integration-sync-facebook-6 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic integration-sync-facebook-7 --partitions 1 --replication-factor 1 --bootstrap-server kafka-1:9092


/opt/kafka/bin/kafka-topics.sh --create --topic ticket-0 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic ticket-1 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic ticket-2 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic ticket-3 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092


/opt/kafka/bin/kafka-topics.sh --create --topic convo-event-0 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic convo-event-1 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic convo-event-2 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic convo-event-3 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic convo-event-4 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic convo-event-5 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic convo-event-6 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic convo-event-7 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092


/opt/kafka/bin/kafka-topics.sh --create --topic send-convo-event-0 --partitions 10 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic send-convo-event-1 --partitions 10 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic send-convo-event-2 --partitions 10 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic send-convo-event-3 --partitions 10 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic send-convo-event-4 --partitions 10 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic send-convo-event-5 --partitions 10 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic send-convo-event-6 --partitions 10 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic send-convo-event-7 --partitions 10 --replication-factor 1 --bootstrap-server kafka-1:9092

scheduler-

/opt/kafka/bin/kafka-topics.sh --create --topic user-event-0 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-event-1 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-event-2 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-event-3 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-event-4 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-event-5 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-event-6 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic user-event-7 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic webhook-event-0 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic webhook-event-1 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic webhook-event-2 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic webhook-event-3 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic webhook-event-4 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic webhook-event-5 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic webhook-event-6 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic webhook-event-7 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-configs.sh --describe --topic webhook-event-7 --all --bootstrap-server kafka-1:9092


/opt/kafka/bin/kafka-topics.sh --create --topic facebook-0 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic facebook-1 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic facebook-2 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic facebook-3 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic metric-count-log-0 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092 
/opt/kafka/bin/kafka-topics.sh --create --topic metric-count-log-1 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092 
/opt/kafka/bin/kafka-topics.sh --create --topic metric-count-log-2 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092 
/opt/kafka/bin/kafka-topics.sh --create --topic metric-count-log-3 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
```

#### List all consumer group
```sh
$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

#### Describe consumber group
```sh
$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group search-1
```

/opt/kafka/bin/kafka-topics.sh --describe --topic send-convo-event-0 --bootstrap-server localhost:9092

/opt/kafka/bin/kafka-topics.sh --create --topic noti5-event --partitions 8 --replication-factor 1 --bootstrap-server kafka-1:9092



/opt/kafka/bin/kafka-topics.sh --create --topic zalo-0 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic zalo-1 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic zalo-2 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic zalo-3 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092



/opt/kafka/bin/kafka-topics.sh --create --topic workflow-event-0 --partitions 8 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic workflow-event-1 --partitions 8 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic workflow-event-2 --partitions 8 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic workflow-event-3 --partitions 8 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic workflow-event-4 --partitions 8 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic workflow-event-5 --partitions 8 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic workflow-event-6 --partitions 8 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic workflow-event-7 --partitions 8 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic integration-updated --partitions 8 --replication-factor 1 --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic counter --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092

# /opt/kafka/bin/kafka-topics.sh --delete --topic counter --bootstrap-server kafka-1:9092

/opt/kafka/bin/kafka-topics.sh --create --topic counter-0 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic counter-1 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic counter-2 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
/opt/kafka/bin/kafka-topics.sh --create --topic counter-3 --partitions 4 --replication-factor 1 --bootstrap-server kafka-1:9092
