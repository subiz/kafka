package kafka

import (
	"bitbucket.org/subiz/gocommon"
	proto "github.com/golang/protobuf/proto"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"os"
	"os/signal"
	"github.com/cenkalti/backoff"
	"regexp"
	"runtime/debug"
	"encoding/gob"
	"bytes"
)

// EventStore publish and listen to kafka events
type EventStore struct {
	consumer *cluster.Consumer
	producer sarama.SyncProducer
}

// Close clean resource
func (me *EventStore) Close() {
	err := me.producer.Close()
	if err != nil {
		common.LogError(err)
	}
}

func newProducer(brokers []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewHashPartitioner
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
}

func validateTopicName(topic string) bool {
	if len(topic) >= 255 {
		return false
	}
	legalTopic := regexp.MustCompile(`^[a-zA-Z0-9\._\-]+$`)
	return legalTopic.MatchString(topic)
}

func encodeGob(str string) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(str)
	if err != nil {
		common.Panic(common.NewInternalErr("%v, unable to encode str %s", err, str))
	}
	return  buf.Bytes()
}

// Publish a event to kafka
// data could be [value] or [value, key]
// value must be type of []byte or proto.Message or string
// string will be encode using encoding/gob
// key must be type string
func (me *EventStore) Publish(topic string, data ...interface{}) {
	var value interface{}
	var key string
	var ok bool
	// convert value from proto.message or string to []byte
	if len(data) == 0 || data[0] == nil {
		common.Panic(common.NewInternalErr("value is nil"))
	} else {
		promes, ok := data[0].(proto.Message)
		if ok {
			value = common.Protify(promes)
		} else {
			value, ok = data[0].([]byte)
			if !ok {
				valuestr, ok := data[0].(string)
				if ok {
					value = encodeGob(valuestr)
				}
				common.Panic(common.NewInternalErr("value should be type of proto.Message, []byte or string, got %v", data[0]))
			}
		}
	}

	// read key
	if len(data) < 1 || data[1] == nil {
		key = ""
	} else {
		key, ok = data[1].(string)
		if !ok {
			common.Panic(common.NewInternalErr("key should be type string, got %v", data[1]))
		}
	}

	if !validateTopicName(topic) {
		common.Panic(common.NewInternalErr("topic is not valid, %s", topic))
	}

	msg := prepareMessage(key, topic, value.([]byte))
	_, _, err := me.producer.SendMessage(msg)
	if err != nil {
		// TODO: this is bad, should we panic or retry
		common.Panic(common.NewInternalErr("%v, unable to send message to kafka, topic: %s, data: %v", err, topic, data))
	}
}

func prepareMessage(key, topic string, message []byte) *sarama.ProducerMessage {
	var msg *sarama.ProducerMessage
	if key == "" {
		msg = &sarama.ProducerMessage{
			Topic: topic,
			Partition: -1,
			Value: sarama.ByteEncoder(message),
		}
	} else {
		msg = &sarama.ProducerMessage{
			Key: sarama.StringEncoder(key),
			Topic: topic,
			Partition: -1,
			Value: sarama.ByteEncoder(message),
		}
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
	for _, t := range topics {
		if !validateTopicName(t) {
			common.Panic(common.NewInternalErr("topic is not valid, %v", t))
		}
	}
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
