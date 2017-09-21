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
//	"sync"
	"time"
	commonpb "bitbucket.org/subiz/servicespec/proto/common"
	"fmt"
	"bitbucket.org/subiz/servicespec/proto/lang"
)

// EventStore publish and listen to kafka events
type EventStore struct {
	consumer *cluster.Consumer
	producer sarama.SyncProducer
	parproducer sarama.SyncProducer
	stopchan chan bool
	cg string
	brokers []string
}

// Close clean resource
func (me *EventStore) Close() {
	err := me.producer.Close()
	if err != nil {
		common.LogError(common.Info{
			"data": err.Error(),
		})
	}
}

func newProducer(brokers []string) (sarama.SyncProducer, sarama.SyncProducer) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	// config.Producer.RequireAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(brokers, config)
	common.PanicIfError(err, lang.T_kafka_error, "unable to create producer with brokers %v", brokers)

	parconfig := sarama.NewConfig()
	parconfig.Producer.Partitioner = sarama.NewManualPartitioner
	parconfig.Producer.Return.Successes = true
	parproducer, err := sarama.NewSyncProducer(brokers, config)
	common.PanicIfError(err, lang.T_kafka_error, "unable to create producer with brokers %v", brokers)
	return producer, parproducer
}

// Connect to kafka brokers
func (me *EventStore) Connect(brokers, topics []string, consumergroup string) {
	if len(topics) != 0 {
		me.consumer = newConsumer(brokers, topics, consumergroup)
	}
	me.brokers = brokers
	me.cg = consumergroup
	me.stopchan = make(chan bool)
	me.producer, me.parproducer = newProducer(brokers)
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
	common.PanicIfError(err, lang.T_internal_error, " unable to encode str %s", str)
	return  buf.Bytes()
}

func (me *EventStore) PublishToPartition(topic string, data interface{}, partition int32) (par int32, offset int64) {
	if !validateTopicName(topic) {
		panic(common.New500(lang.T_topic_is_empty, "topic should not be empty or contains invalid characters, got %s", topic))
	}
	var value []byte
	var ok bool

	promes, ok := data.(proto.Message)
	if ok {
		value = common.Protify(promes)
	} else {
		value, ok = data.([]byte)
		if !ok {
			valuestr, ok := data.(string)
			if !ok {
				panic(common.New500(lang.T_wrong_type, "value should be type of proto.Message, []byte or string, got %v", data))
			}
			value = []byte(valuestr)//encodeGob(valuestr)
		}
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Partition: partition,
		Value: sarama.ByteEncoder(value),
	}
	var err error
	par, offset, err = me.parproducer.SendMessage(msg)
	if err != nil {
		common.Log(nil, topic, data)
		// TODO: this is bad, should we panic or retry
		panic(common.New500(lang.T_unable_to_send_message, "%v, topic: %s, data: %v", err, topic, data))
	}
	return par, offset
}

// Publish with context tracing
func (me *EventStore) PublishT(ctx commonpb.Context, topic string, data ...interface{}) (par int32, offset int64) {
	return me.Publish(topic, data...)
}

// Publish a event to kafka
// data could be [value] or [value, key]
// value must be type of []byte or proto.Message or string
// string will be encode using encoding/gob
// key must be type string
func (me *EventStore) Publish(topic string, data ...interface{}) (partition int32, offset int64) {
	if !validateTopicName(topic) {
		panic(common.New500(lang.T_topic_is_empty, "topic should not be empty or contains invalid characters, got %s", topic))
	}
	var value []byte
	var key string
	var ok bool
	// convert value from proto.message or string to []byte
	if len(data) == 0 || data[0] == nil {
		panic(common.New500(lang.T_empty, "value is nil"))
	}

	promes, ok := data[0].(proto.Message)
	if ok {
		value = common.Protify(promes)
	} else {
		value, ok = data[0].([]byte)
		if !ok {
			valuestr, ok := data[0].(string)
			if !ok {
				panic(common.New500(lang.T_wrong_type, "value should be type of proto.Message, []byte or string, got %v", data[0]))
			}
			value = []byte(valuestr)//encodeGob(valuestr)
		}
	}

	// read key
	if len(data) < 2 || data[1] == nil {
		key = ""
	} else {
		key, ok = data[1].(string)
		if !ok {
			panic(common.New500(lang.T_wrong_type, "key should be type string, got %v", data[1]))
		}
	}

	msg := prepareMessage(key, topic, value)
	partition, offset, err := me.producer.SendMessage(msg)
	if err != nil {
		common.Log(nil, topic, data)
		// TODO: this is bad, should we panic or retry
		panic(common.New500(lang.T_unable_to_send_message, "%v, topic: %s, data: %v", err, topic, data))
	}
	return partition, offset
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
			//Partition: -1,
			Value: sarama.ByteEncoder(message),
		}
	}
	return msg
}

// Handler ...
// NotiHandler ...

// Listen start listening kafka consumer
func (me *EventStore) Listen(h func(partition int32, topic string, value []byte, offset int64) bool, cbs ...interface{}) {
	var nhok bool
	var nh  func(map[string][]int32)
	if len(cbs) > 0 {
		nh, nhok = cbs[0].(func(map[string][]int32))
		if !nhok {
			debug.PrintStack()
			panic("parameter 2 should have type of type func(map[string][]int32)")
		}
	}

	firstmessage := true
	for {	select {
	case msg, more := <-me.consumer.Messages():
		if !more {
			goto end
		}

		if firstmessage && nh != nil {
			firstmessage = false
			go nh(me.consumer.Subscriptions())
		}
		out := func() bool { // don't allow this function die
			defer func() {
				r := recover()
				if r != nil {
					debug.PrintStack()
					common.LogError(common.Info{
						"r": fmt.Sprintf("%v", r),
					})
				}
			}()
			return h(msg.Partition, msg.Topic, msg.Value, msg.Offset)
		}()
		me.consumer.MarkOffset(msg, "")
		if !out {
			goto end
		}
	case err, more := <-me.consumer.Errors():
		if !more {

			goto end
		}
		common.LogError(common.Info{
			"err": fmt.Sprintf("%v", err),
		})
	case ntf, more := <-me.consumer.Notifications():
		if !more {
			goto end
		}
		if nhok {
			firstmessage = false
			nh(ntf.Current)
		}
	case <-EndSignal():
		goto end
	}}
end:
	err := me.consumer.Close()
	common.Panic(err)
}

func (me *EventStore) CloseConsumer() {
	me.consumer.Close()
}

func newConsumer(brokers, topics []string, consumergroup string) *cluster.Consumer {
	for _, t := range topics {
		if !validateTopicName(t) {
			common.PanicIfError(&common.Error{}, lang.T_invalid_kafka_topic, "topic is not valid, %v", t)
		}
	}
	c := cluster.NewConfig()
	//c.Consumer.MaxWaitTime = 10000 * time.Millisecond
	//c.Consumer.Offsets.CommitInterval = 1 * time.Millisecond
	//c.Consumer.Offsets.Retention = 0
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Group.Session.Timeout = 6 * time.Second
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

func EndSignal() chan os.Signal {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	return signals
}
