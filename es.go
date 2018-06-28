package kafka

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"

	commonpb "bitbucket.org/subiz/header/common"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
)

var partitioner = sarama.NewHashPartitioner("")

// EventStore publish and listen to kafka events
type EventStore struct {
	consumer      *cluster.Consumer
	producer      sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	stopchan      chan bool
	cg            string
	brokers       []string
}

// Close clean resource
func (me EventStore) Close() {
	if err := me.producer.Close(); err != nil {
		log.Println("kafka producer close error", err)
	}
	if err := me.asyncProducer.Close(); err != nil {
		log.Println("kafka async close error", err)
	}
}

func newProducer(brokers []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	// config.Producer.RequireAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(fmt.Sprintf("unable to create producer with brokers %v", brokers))
	}
	return producer
}

func newAsyncProducer(brokers []string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	// config.Producer.RequireAcks = sarama.WaitForAll
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("unable to create async producer with brokers %v", brokers)
	}
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
	me.asyncProducer = newAsyncProducer(brokers)
	me.listenAsyncProducerEvents()
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
	msg := createMessage(topic, data, key)
	for {
		var err error
		partition, offset, err = me.producer.SendMessage(&msg)
		if err == nil {
			break
		}
		log.Println(err)
		log.Printf("unable to publist message, topic: %s, data: %v\n", topic, data)
		log.Println("retrying after 5sec")
		time.Sleep(5 * time.Second)
	}
	return partition, offset
}

func (me *EventStore) AsyncPublish(topic string, data interface{}, key string) {
	msg := createMessage(topic, data, key)
	me.asyncProducer.Input() <- &msg
}

func createMessage(topic string, data interface{}, key string) sarama.ProducerMessage {
	var value []byte
	// convert value from proto.message or string to []byte
	if data == nil {
		panic("value is nil")
	}
	var err error
	switch data := data.(type) {
	case proto.Message:
		value, err = proto.Marshal(data)
		if err != nil {
			panic(fmt.Sprintf("unable to protify struct %v", data))
		}
	case []byte:
		value = data
	case string:
		value = []byte(data)
	default:
		panic(fmt.Sprintf("value should be type of proto.Message, []byte or string, got %v", data))
	}

	msg := prepareMessage(key, topic)
	msg.Value = sarama.ByteEncoder(value)
	return msg
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

// Listen start listening kafka consumer
func (me EventStore) Listen(h func(partition int32, topic string, value []byte, offset int64) bool, cbs ...interface{}) {
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
				defer func() {
					if r := recover(); r != nil {
						log.Println(r)
					}
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
			if err != nil {
				log.Println("kafka err", err)
			}
		case <-EndSignal():
			goto end
		}
	}
end:
	log.Println("STOPED==============================")
	if err := me.consumer.Close(); err != nil {
		panic(fmt.Sprintf("unable to close consumer: %v", err))
	}
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
			panic(fmt.Sprintf("topic is not valid, %v", topics[i]))
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
			log.Println(err, "will retry...")
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
	if err != nil {
		panic(fmt.Sprintf("unable to hash key %s to partition", key))
	}
	return par
}

func (me *EventStore) listenAsyncProducerEvents() {
	go func() {
		var successes uint
		for range me.asyncProducer.Successes() {
			successes++
		}
	}()

	go func() {
		for err := range me.asyncProducer.Errors() {
			log.Printf("async publish message error: %s", err)
			log.Println(err)
		}
	}()
}
