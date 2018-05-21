package kafka

import (
	"bitbucket.org/subiz/gocommon"
	commonpb "bitbucket.org/subiz/header/common"
	"bitbucket.org/subiz/header/lang"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"
)

var partitioner = sarama.NewHashPartitioner("")

// EventStore publish and listen to kafka events
type EventStore struct {
	consumer *cluster.Consumer
	producer sarama.SyncProducer
	stopchan chan bool
	cg       string
	brokers  []string
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
func (me *EventStore) Connect(brokers, topics []string, consumergroup string, frombegin ...bool) {
	fb := true
	if len(frombegin) == 1 {
		fb = frombegin[0]
	}
	if len(topics) != 0 {
		me.consumer = newConsumer(brokers, topics, consumergroup, fb)
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
	var value []byte
	// convert value from proto.message or string to []byte
	if data == nil {
		panic(common.New500(lang.T_empty, "value is nil"))
	}
	var err error
	switch data := data.(type) {
	case proto.Message:
		value, err = proto.Marshal(data)
		if err != nil {
			panic(common.New500(lang.T_wrong_type, "unable to protify struct %v", data))
		}
	case []byte:
		value = data
	case string:
		value = []byte(data)
	default:
		panic(common.New500(lang.T_wrong_type, "value should be type of proto.Message, []byte or string, got %v", data))
	}

	msg := prepareMessage(key, topic)
	msg.Value = sarama.ByteEncoder(value)
	for {
		partition, offset, err = me.producer.SendMessage(&msg)
		if err == nil {
			break
		}
		common.LogErr(err)
		common.Log("unable to publist message, topic: %s, data: %v", topic, data)
		common.Log("retrying after 5sec")
		time.Sleep(5 * time.Second)
	}
	return partition, offset
}

func prepareMessage(key, topic string) sarama.ProducerMessage {
	if key == "" {
		return sarama.ProducerMessage{
			Topic:     topic,
			Partition: -1,
		}
	}
	return sarama.ProducerMessage{
		Key:   sarama.StringEncoder(key),
		Topic: topic,
	}
}

func (me EventStore) CommitManually() error {
	return me.consumer.CommitOffsets()
}

func (me EventStore) MarkOffset(topic string, par int32, offset int64) {
	me.consumer.MarkOffset(&sarama.ConsumerMessage{
		Topic:     topic,
		Partition: par,
		Offset:    offset,
	}, "")
}

// Peek just like Listen but do not commit
func (me EventStore) Peek(h func(string, int32, int64, []byte) bool, cbs ...interface{}) {
	defer common.Recover()
	var nh func(map[string][]int32)
	if len(cbs) > 0 {
		nh = cbs[0].(func(map[string][]int32))
	}

	for {
		select {
		case msg, more := <-me.consumer.Messages():
			if !more {
				goto end
			}
			if false == func() bool { // don't allow this function die
				defer common.Recover()
				return h(msg.Topic, msg.Partition, msg.Offset, msg.Value)
			}() {
				goto end
			}
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
		}
	}
end:
	common.Log("STOPED==============================")
	err := me.consumer.Close()
	common.DieIf(err, lang.T_kafka_error, "unable to close consumer")
}

// Listen start listening kafka consumer
func (me EventStore) Listen(h func(partition int32, topic string, value []byte, offset int64) bool, cbs ...interface{}) {
	defer common.Recover()
	var nh func(map[string][]int32)
	if len(cbs) > 0 {
		nh = cbs[0].(func(map[string][]int32))
	}

	for {
		select {
		case msg, more := <-me.consumer.Messages():
			if !more {
				goto end
			}
			out := func() bool { // don't allow this function die
				defer common.Recover()
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
		}
	}
end:
	common.Log("STOPED==============================")
	err := me.consumer.Close()
	common.DieIf(err, lang.T_kafka_error, "unable to close consumer")
}

func (me *EventStore) CloseConsumer() {
	me.consumer.Close()
}

func (me *EventStore) GetConsumer() *cluster.Consumer {
	return me.consumer
}

func (me *EventStore) GetConsumerSubscriptions() map[string][]int32 {
	return me.consumer.Subscriptions()
}

func (me *EventStore) EnableSaramaLogging() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
}

func newConsumer(brokers, topics []string, consumergroup string, frombegin bool) *cluster.Consumer {
	for i := range topics {
		topics[i] = strings.TrimSpace(topics[i])
		if !validateTopicName(topics[i]) {
			panic(common.New500(lang.T_invalid_kafka_topic, "topic is not valid, %v", topics[i]))
		}
	}
	c := cluster.NewConfig()
	//c.Consumer.MaxWaitTime = 10000 * time.Millisecond
	//c.Consumer.Offsets.CommitInterval = 1 * time.Millisecond
	//c.Consumer.Offsets.Retention = 0
	c.Consumer.Return.Errors = true
	if frombegin {
		c.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		c.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	c.Group.Session.Timeout = 10 * time.Second
	c.Group.Return.Notifications = true

	var err error
	var consumer *cluster.Consumer
	for {
		consumer, err = cluster.NewConsumer(brokers, consumergroup, topics, c)
		if err != nil {
			common.Log(err, "will retry...")
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}

	return consumer
}

func EndSignal() chan os.Signal {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	return signals
}

func HashKeyToPar(N int, key string) int32 {
	par, err := partitioner.Partition(&sarama.ProducerMessage{}, int32(N))
	common.DieIf(err, lang.T_kafka_error, "unable to hash key %s to partition", key)
	return par
}
