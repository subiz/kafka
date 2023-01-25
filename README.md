
### Mount disk
```sh
$ lsblk
NAME    MAJ:MIN RM   SIZE RO TYPE MOUNTPOINTS
loop0     7:0    0  55.6M  1 loop /snap/core18/2667
loop1     7:1    0  63.3M  1 loop /snap/core20/1778
loop2     7:2    0 295.6M  1 loop /snap/google-cloud-cli/95
loop3     7:3    0   103M  1 loop /snap/lxd/23541
loop4     7:4    0  49.6M  1 loop /snap/snapd/17883
sda       8:0    0    50G  0 disk
├─sda1    8:1    0  49.9G  0 part /
├─sda14   8:14   0     4M  0 part
└─sda15   8:15   0   106M  0 part /boot/efi
sdb       8:16   0   300G  0 disk

# sudo mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb
# sudo mkdir -p /data
# sudo mount -o discard,defaults /dev/sdb /data
# sudo chmod a+w /data

```
Config mount disk automatically after restart

```sh
# sudo cp /etc/fstab /etc/fstab.backup
# sudo blkid /dev/sdb
/dev/sdb: UUID="c26abef3-34ef-4411-85b9-b4ef8ad18ead" BLOCK_SIZE="4096" TYPE="ext4"
```
Edit `/etc/fstab`, append
```
UUID="c26abef3-34ef-4411-85b9-b4ef8ad18ead" /data ext4 discard,defaults,MOUNT_OPTION 0 2
```

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
$ bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
```
Start the Kafka Server
`
$ bin/kafka-server-start.sh config/kraft/server.properties
`
Once the Kafka server has successfully launched, you will have a basic Kafka environment running and ready to use.


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
/opt/kafka/bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
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
