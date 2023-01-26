package kafka

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"google.golang.org/protobuf/proto"
)

var producer sarama.SyncProducer
var hashedproducer sarama.SyncProducer
var started bool
var lock = &sync.Mutex{}
var g_brokers = []string{"kafka-1:9092"}

func SetBrokers(brokers []string) {
	g_brokers = brokers
}

func prepareProducer() {
	if started {
		return
	}

	lock.Lock()
	if started {
		lock.Unlock()
		return
	}

	var err error
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	brokers := g_brokers
	producer, err = sarama.NewSyncProducer(brokers, config)
	for err != nil {
		log.Println("can't create sync producer to ", brokers, "retries in 5 sec", err)
		time.Sleep(5 * time.Second)
		producer, err = sarama.NewSyncProducer(brokers, config)
	}

	config = sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	hashedproducer, err = sarama.NewSyncProducer(brokers, config)
	for err != nil {
		log.Println("can't create sync producer to ", brokers, "retries in 5 sec", err)
		time.Sleep(5 * time.Second)
		hashedproducer, err = sarama.NewSyncProducer(brokers, config)
	}
	started = true
	lock.Unlock()
}

func prepareMsg(topic string, data interface{}, par int32, key string) *sarama.ProducerMessage {
	var value []byte
	if data == nil {
		log.Println("value is nil")
		return nil
	}

	var err error
	switch data := data.(type) {
	case proto.Message:
		value, err = proto.Marshal(data)
		if err != nil {
			return nil
		}
	case []byte:
		value = data
	case string:
		value = []byte(data)
	default:
		log.Println("value should be type of proto.Message, []byte or string")
		return nil
	}

	return &sarama.ProducerMessage{
		Topic:     topic,
		Partition: par,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(value),
	}
}

func Publish(topic string, data interface{}, keys ...string) {
	var key string
	if len(keys) > 0 {
		key = keys[0]
	}
	PublishToPartition(topic, data, -1, key)
}

func PublishToPartition(topic string, data interface{}, par int32, key string) {
	prepareProducer()

	msg := prepareMsg(topic, data, par, key)
	if msg == nil {
		log.Println("no publish")
		return
	}
	prod := producer
	if par == -1 {
		prod = hashedproducer
	}
	for {
		_, _, err := prod.SendMessage(msg)
		if err == nil {
			break
		}

		d := fmt.Sprintf("%v", data)
		if len(d) > 5000 {
			d = d[len(d)-4000:]
		}
		log.Printf("unable to publish message, topic: %s, partition %d, key %s, data: %s %v\n", topic, par, key, d, err)
		if err.Error() == sarama.ErrInvalidPartition.Error() ||
			err.Error() == sarama.ErrMessageSizeTooLarge.Error() {
			break
		}

		log.Println("retrying after 5sec")
		time.Sleep(5 * time.Second)
	}
}
