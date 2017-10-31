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
	"time"
	commonpb "bitbucket.org/subiz/header/common"
	"bitbucket.org/subiz/header/lang"
)

// EventStore publish and listen to kafka events
type EventStore struct {
	consumer *cluster.Consumer
	producer sarama.SyncProducer
	stopchan chan bool
	cg string
	brokers []string
}

// Close clean resource
func (me EventStore) Close() {
	err := me.producer.Close()
	common.LogErr(err)
}

func newProducer(brokers []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	// config.Producer.RequireAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(brokers, config)
	common.DieIf(err, lang.T_kafka_error, "unable to create producer with brokers %v", brokers)

	return producer
}

// Connect to kafka brokers
func (me *EventStore) Connect(brokers, topics []string, consumergroup string) {
	if len(topics) != 0 {
		me.consumer = newConsumer(brokers, topics, consumergroup)
	}
	me.brokers = brokers
	me.cg = consumergroup
	me.stopchan = make(chan bool)
	me.producer = newProducer(brokers)
}

func validateTopicName(topic string) bool {
	if len(topic) >= 255 {
		return false
	}
	legalTopic := regexp.MustCompile(`^[a-zA-Z0-9\._\-]+$`)
	return legalTopic.MatchString(topic)
}

// Publish with context tracing
func (me EventStore) PublishT(ctx commonpb.Context, topic string, data interface{}, key string) (par int32, offset int64) {
	return me.Publish(topic, data, key)
}

// Publish a event to kafka
// data must be type of []byte or proto.Message or string
// string will be encode using encoding/gob
func (me EventStore) Publish(topic string, data interface{}, key string) (partition int32, offset int64) {
	if !validateTopicName(topic) {
		panic(common.New500(lang.T_topic_is_empty, "topic should not be empty or contains invalid characters, got %s", topic))
	}
	var value []byte
	// convert value from proto.message or string to []byte
	if data == nil {
		panic(common.New500(lang.T_empty, "value is nil"))
	}

	switch dat := data.(type) {
	case proto.Message:
		value = common.Protify(dat)
	case []byte:
		value = dat
	case string:
		value = []byte(dat)
	default:
		panic(common.New500(lang.T_wrong_type, "value should be type of proto.Message, []byte or string, got %v", data))
	}

	msg := prepareMessage(key, topic)
	msg.Value = sarama.ByteEncoder(value)
	partition, offset, err := me.producer.SendMessage(&msg)
	if err != nil {
		common.Log(nil, topic, data)
		// TODO: this is bad, should we panic or retry
		panic(common.New500(lang.T_unable_to_send_message, "%v, topic: %s, data: %v", err, topic, data))
	}
	return partition, offset
}

func prepareMessage(key, topic string) sarama.ProducerMessage {
	if key == "" {
		return sarama.ProducerMessage{
			Topic: topic,
			Partition: -1,
		}
	}
	return sarama.ProducerMessage{
		Key: sarama.StringEncoder(key),
		Topic: topic,
	}
}

// Listen start listening kafka consumer
func (me EventStore) Listen(h func(partition int32, topic string, value []byte, offset int64) bool, cbs ...interface{}) {
	var nh func(map[string][]int32)
	if len(cbs) > 0 {
		nh = cbs[0].(func(map[string][]int32))
	}

	for {	select {
	case msg, more := <-me.consumer.Messages():
		if !more {
			goto end
		}
		out := func() bool { // don't allow this function die
			defer func() {
				r := recover()
				common.LogErr(r)
			}()
			return h(msg.Partition, msg.Topic, msg.Value, msg.Offset)
		}()

		if !out {
			goto end
		}
		me.consumer.MarkOffset(msg, "")
	case ntf, more := <-me.consumer.Notifications():
		if !more {
			goto end
		}
		if nh != nil {
			nh(ntf.Current)
		}
	case err, more := <-me.consumer.Errors():
		if !more {
			goto end
		}
		common.LogErr(err)
	case <-EndSignal():
		goto end
	}}
end:
	err := me.consumer.Close()
	common.DieIf(err, lang.T_kafka_error, "unable to close consumer")
}

func (me *EventStore) CloseConsumer() {
	me.consumer.Close()
}

func newConsumer(brokers, topics []string, consumergroup string) *cluster.Consumer {
	for _, t := range topics {
		if !validateTopicName(t) {
			panic(common.New500(lang.T_invalid_kafka_topic, "topic is not valid, %v", t))
		}
	}
	c := cluster.NewConfig()
	//c.Consumer.MaxWaitTime = 10000 * time.Millisecond
	//c.Consumer.Offsets.CommitInterval = 1 * time.Millisecond
	//c.Consumer.Offsets.Retention = 0
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Group.Session.Timeout = 6 * time.Second
	c.Group.Return.Notifications = true

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
	common.DieIf(err, lang.T_kafka_error, "unable to create consumer with brokers %v", brokers)
	return consumer
}

func EndSignal() chan os.Signal {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	return signals
}
