
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
$ /opt/kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
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
Also, change the listeners key in `/opt/kafka/config/kraft/broker.properties` and `/opt/kafka/config/kraft/server.properties`

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
