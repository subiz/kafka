package kafka

import (
	"bitbucket.org/subiz/gocommon"
	proto "github.com/golang/protobuf/proto"
	"github.com/Shopify/sarama"
		cluster "github.com/bsm/sarama-cluster"
	"os"
	"os/signal"
	"time"
)

// EventStore publish and listen to kafka events
type EventStore struct {
	consumer *cluster.Consumer
	producer sarama.SyncProducer
}

func newProducer(brokers []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
//	config.Producer.RequireAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(brokers, config)
	common.Panicf(err, "unable to create producer with brokers %v", brokers)
	return producer
}

func (me *EventStore) Connect(brokers, topics []string, consumergroup string) {
	if len(topics) != 0 {
		me.consumer = newConsumer(brokers, topics, consumergroup)
	}
	me.producer = newProducer(brokers)
}

// Publish publish job
func (me *EventStore) Publish(topic string, data interface{}) {
	promes, ok := data.(proto.Message)
	if ok {
		data = common.Protify(promes)
	}
	msg := prepareMessage(topic, data.([]byte))
	_, _, err := me.producer.SendMessage(msg)
	if err != nil {
		common.Panic(common.NewInternalErr("%v, unable to send message to kafka, topic: %s, data: %v", err, topic, data))
	}
}

func prepareMessage(topic string, message []byte) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Partition: -1,
		Value: sarama.ByteEncoder(message),
	}
	return msg
}

type handler func(string, []byte)

// Listen start listening kafka consumer
func (me *EventStore) Listen(h handler) {
	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume messages, watch errors and notifications
	for { select {
	case msg, more := <-me.consumer.Messages():
		if !more { break }
		func() { // don't this function die because of panic
			defer func() {
				r := recover()
				if r != nil {
					common.LogError(r)
				}
			}()
			h(msg.Topic, msg.Value)
		}()
		me.consumer.MarkOffset(msg, "")
	case err, more := <-me.consumer.Errors():
		if !more { break }
		common.LogError("%v", err)
	case ntf, more := <-me.consumer.Notifications():
		if !more { break }
		common.Log("Kafka Rebalanced: %+v\n", ntf)
	case <-signals:
		return
	}}
}

func newConsumer(brokers, topics []string, consumergroup string) *cluster.Consumer {
	c := cluster.NewConfig()
	c.Metadata.Retry.Max = 3
	c.Metadata.Retry.Backoff = 250 * time.Millisecond
	//c.Metadata.RefreshFrequency = 10 * time.Minute

	c.Consumer.Return.Errors = true
	//c.Consumer.Retry.Backoff = 2 * time.Minute
	//c.Consumer.MaxWaitTime = 2 * time.Minute //250 * time.Millisecond
	//c.Consumer.MaxProcessingTime = 100 * time.Minute//time.Millisecond
	//c.Consumer.Offsets.CommitInterval = 1 * time.Minute//time.Second
	//c.Consumer.Offsets.Initial = OffsetNewest
	c.Group.Return.Notifications = true
	common.Log(topics)
	consumer, err := cluster.NewConsumer(brokers, consumergroup, topics, c)
	common.Panicf(err, "unable to create consumer with brokers %v", brokers)
	return consumer
}
