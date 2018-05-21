package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"time"
)

type Publisher struct {
	brokers        []string
	producer       sarama.SyncProducer
	hashedproducer sarama.SyncProducer
}

func NewPublisher(brokers []string) *Publisher {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	for err != nil {
		fmt.Println("can't create sync producer to ", brokers, "retries in 5 sec")
		time.Sleep(5 * time.Second)
		producer, err = sarama.NewSyncProducer(brokers, config)
	}

	config = sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewHashPartitioner

	config.Producer.Return.Successes = true
	hashedproducer, err := sarama.NewSyncProducer(brokers, config)
	for err != nil {
		fmt.Println("can't create sync producer to ", brokers, "retries in 5 sec")
		time.Sleep(5 * time.Second)
		producer, err = sarama.NewSyncProducer(brokers, config)
	}

	return &Publisher{brokers: brokers, producer: producer, hashedproducer: hashedproducer}
}

func (p *Publisher) Publish(topic string, data interface{}, par int32, key string) {
	var value []byte
	if data == nil {
		fmt.Println("value is nil")
		return
	}

	var err error
	switch data := data.(type) {
	case proto.Message:
		value, err = proto.Marshal(data)
		if err != nil {
			fmt.Println(data)
			panic(err)
		}
	case []byte:
		value = data
	case string:
		value = []byte(data)
	default:
		panic("value should be type of proto.Message, []byte or string")
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: par,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(value),
	}

	prod := p.producer
	if par == -1 {
		prod = p.hashedproducer
	}
	for {
		if _, _, err = prod.SendMessage(msg); err == nil {
			break
		}
		fmt.Println(err)
		fmt.Printf("unable to publist message, topic: %s, data: %v\n", topic, data)
		fmt.Println("retrying after 5sec")
		time.Sleep(5 * time.Second)
	}
}
