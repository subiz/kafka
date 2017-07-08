package kafka

import (
	"bitbucket.org/subiz/gocommon"
	proto "github.com/golang/protobuf/proto"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"os"
	"os/signal"
	"github.com/cenkalti/backoff"
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
	// config.Producer.RequireAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(brokers, config)
	common.Panicf(err, "unable to create producer with brokers %v", brokers)
	return producer
}

// Connect to kafka brokers
func (me *EventStore) Connect(brokers, topics []string, consumergroup string) {
	if len(topics) != 0 {
		me.consumer = newConsumer(brokers, topics, consumergroup)
	}
	me.producer = newProducer(brokers)
}

// Publish job
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
		func() { // don't allow this function die
			defer func() {
				r := recover()
				if r != nil {
					debug.PrintStack()
					common.LogError(r)
				}
			}()
			h(msg.Topic, msg.Value)
		}()
		me.consumer.MarkOffset(msg, "")
	case err, more := <-me.consumer.Errors():
		if !more { break }
		common.LogError("%v", err)
		//case ntf, more := <-me.consumer.Notifications():
		//	if !more { break }
		//common.Log("Kafka Rebalanced: %+v\n", ntf)
	case <-signals:
		return
	}}
}

func newConsumer(brokers, topics []string, consumergroup string) *cluster.Consumer {
	c := cluster.NewConfig()
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	//c.Group.Return.Notifications = true
	//common.Log(topics)

	ticker := backoff.NewTicker(backoff.NewExponentialBackOff())
	var err error
	var consumer *cluster.Consumer
	for range ticker.C {
		consumer, err = cluster.NewConsumer(brokers, consumergroup, topics, c)
		if err != nil {
			common.Log(err, "will retry...")
			continue
		}
		ticker.Stop()
		break
	}
	common.Panicf(err, "unable to create consumer with brokers %v", brokers)
	return consumer
}
