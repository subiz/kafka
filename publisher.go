package kafka

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
)

type Publisher struct {
	brokers             []string
	producer            sarama.SyncProducer
	hashedproducer      sarama.SyncProducer
	asyncproducer       sarama.AsyncProducer
	hashedasyncproducer sarama.AsyncProducer
}

func newHashedSyncPublisher(brokers []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	for err != nil {
		log.Println("can't create sync producer to ", brokers, "retries in 5 sec")
		time.Sleep(5 * time.Second)
		producer, err = sarama.NewSyncProducer(brokers, config)
}
	return producer
}

func newSyncPublisher(brokers []string) sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	for err != nil {
		log.Println("can't create sync producer to ", brokers, "retries in 5 sec")
		time.Sleep(5 * time.Second)
		producer, err = sarama.NewSyncProducer(brokers, config)
	}
	return producer
}

func newAsyncPublisher(brokers []string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner

	config.Producer.Return.Successes = true
	// config.Producer.RequireAcks = sarama.WaitForAll
	producer, err := sarama.NewAsyncProducer(brokers, config)
	for err != nil {
		log.Println("can't create async producer to ", brokers, "retries in 5 sec")
		time.Sleep(5 * time.Second)
		producer, err = sarama.NewAsyncProducer(brokers, config)
	}
	return producer
}

func newHashedAsyncPublisher(brokers []string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewHashPartitioner

	config.Producer.Return.Successes = true
	// config.Producer.RequireAcks = sarama.WaitForAll
	producer, err := sarama.NewAsyncProducer(brokers, config)
	for err != nil {
		log.Println("can't create async producer to ", brokers, "retries in 5 sec")
		time.Sleep(5 * time.Second)
		producer, err = sarama.NewAsyncProducer(brokers, config)
	}
	return producer
}

func NewPublisher(brokers []string) *Publisher {
	p := &Publisher{
		brokers:             brokers,
		producer:            newSyncPublisher(brokers),
		hashedproducer:      newHashedSyncPublisher(brokers),
		asyncproducer:       newAsyncPublisher(brokers),
		hashedasyncproducer: newHashedAsyncPublisher(brokers),
	}
	go p.listenAsyncProducerErrors()
	return p
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
			log.Printf("kafka error, unable to marshal: %v\n", data)
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

func (p *Publisher) PublishAsync(topic string, data interface{}, par int32, key string) {
	msg := prepareMsg(topic, data, par, key)
	if msg == nil {
		log.Println("no publish")
		return
	}
	prod := p.asyncproducer
	if par == -1 {
		prod = p.hashedasyncproducer
	}
	prod.Input() <- msg
}

func (p *Publisher) listenAsyncProducerErrors() {
	for {
		select {
		case <-p.asyncproducer.Successes():
		case <-p.hashedasyncproducer.Successes():
		case err := <-p.asyncproducer.Errors():
			log.Printf("async publish message error: %s\n", err)
			log.Println(err)

		case err := <-p.hashedasyncproducer.Errors():
			log.Printf("async publish message error: %s\n", err)
			log.Println(err)
		}
	}
}

func (p *Publisher) Publish(topic string, data interface{}, par int32, key string) {
	msg := prepareMsg(topic, data, par, key)
	if msg == nil {
		log.Println("no publish")
		return
	}
	prod := p.producer
	if par == -1 {
		prod = p.hashedproducer
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
