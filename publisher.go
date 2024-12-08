package kafka

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

var producerM = map[string]sarama.SyncProducer{}
var hashedproducerM = map[string]sarama.SyncProducer{}

var lock = &sync.Mutex{}

func prepareProducer(broker string, par int) sarama.SyncProducer {
	var prod sarama.SyncProducer
	if par == -1 {
		prod = hashedproducerM[broker]
	} else {
		prod = producerM[broker]
	}

	if prod != nil {
		return prod
	}

	lock.Lock()
	defer lock.Unlock()

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.MaxMessageBytes = 10 * 1024 * 1024
	config.Producer.Retry.Max = 10
	var pM map[string]sarama.SyncProducer
	if par == -1 {
		config.Producer.Partitioner = sarama.NewHashPartitioner
		pM = hashedproducerM
	} else {
		config.Producer.Partitioner = sarama.NewManualPartitioner
		pM = producerM
	}

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	for err != nil {
		log.Println("can't create sync producer to ", broker, "retries in 5 sec", err)
		time.Sleep(5 * time.Second)
		producer, err = sarama.NewSyncProducer([]string{broker}, config)
	}

	newpM := map[string]sarama.SyncProducer{}
	for k, v := range pM {
		newpM[k] = v
	}
	newpM[broker] = producer

	if par == -1 {
		hashedproducerM = newpM
	} else {
		producerM = newpM
	}
	return producer
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

func Publish(broker string, topic string, data interface{}, keys ...string) {
	var key string
	if len(keys) > 0 {
		key = keys[0]
	} else {
		// random
		key = strconv.Itoa(int(time.Now().UnixNano()))
	}
	PublishToPartition(broker, topic, data, -1, key)
}

func PublishToPartition(broker, topic string, data interface{}, par int32, key string) {
	prod := prepareProducer(broker, int(par))
	msg := prepareMsg(topic, data, par, key)
	if msg == nil {
		log.Println("no publish")
		return
	}
	for i := 0; i < 10; i++ {
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
