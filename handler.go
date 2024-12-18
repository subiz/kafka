package kafka

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/paulbellamy/ratecounter"
	"github.com/subiz/log"
)

// var hostname string // search-n
type HandlerFunc func(partition int32, offset int64, data []byte, key string)
type PartitionHandlerFunc func(offset int64, data []byte)

var g_consumer_group_session_lock = &sync.Mutex{}
var g_consumer_group_session = map[string]sarama.ConsumerGroupSession{}

// deprecated, use Listen2
// Serve listens messages from kafka and call matched handlers
func Listen(broker, consumerGroup, topic string, handleFunc HandlerFunc) error {
	if topic == "" {
		return log.ERetry(nil, log.M{"message": "topic cannot be empty"})
	}
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	//  config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	counter := ratecounter.NewRateCounter(1 * time.Minute)
	go func() {
		for {
			time.Sleep(120 * time.Second)
			log.Info("subiz", "KAFKA RATE", topic, counter.Rate())
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	con := newConsumer(broker, consumerGroup, topic, handleFunc, counter)
	client, err := sarama.NewConsumerGroup([]string{broker}, consumerGroup, config)
	if err != nil {
		cancel()
		return err
	}
	go func() {
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, []string{topic}, con); err != nil {
				log.Err("subiz", err, "KAFKA ERR", topic, consumerGroup)
				time.Sleep(10 * time.Second)
				continue
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	cancel()
	return client.Close()
}

// Listen2 likes Listen but auto commit
func Listen2(addr, consumerGroup, topic string, handleFunc HandlerFunc) error {
	lock := sync.Mutex{}
	var latestConsumeOffset = map[int32]int64{}

	go func() {
		for {
			time.Sleep(1 * time.Minute)
			lock.Lock()
			for partition, offset := range latestConsumeOffset {
				MarkOffset(addr, consumerGroup, topic, partition, offset+1)
			}
			latestConsumeOffset = map[int32]int64{}
			lock.Unlock()
		}
	}()
	myFunc := func(partition int32, offset int64, data []byte, key string) {
		handleFunc(partition, offset, data, key)
		lock.Lock()
		latestConsumeOffset[partition] = offset
		lock.Unlock()
	}
	return Listen(addr, consumerGroup, topic, myFunc)
}

// offset + 1
func MarkOffset(broker, consumerGroup, topic string, partition int32, offset int64) {
	g_consumer_group_session_lock.Lock()
	defer g_consumer_group_session_lock.Unlock()
	session := g_consumer_group_session[broker+","+consumerGroup+","+topic]
	if session == nil {
		return
	}
	session.MarkOffset(topic, partition, offset, "")
}

// Consumer represents a Sarama consumer group consumer
type consumer struct {
	broker        string
	topic         string
	consumerGroup string
	handler       HandlerFunc
	counter       *ratecounter.RateCounter
}

func newConsumer(broker, consumerGroup, topic string, handler HandlerFunc, counter *ratecounter.RateCounter) *consumer {
	return &consumer{broker: broker, topic: topic, consumerGroup: consumerGroup, handler: handler, counter: counter}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (me *consumer) Setup(session sarama.ConsumerGroupSession) error {
	g_consumer_group_session_lock.Lock()
	g_consumer_group_session[me.broker+","+me.consumerGroup+","+me.topic] = session
	g_consumer_group_session_lock.Unlock()
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (*consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (me *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, more := <-claim.Messages():
			if !more {
				return nil
			}
			if message == nil {
				break
			}
			me.counter.Incr(1)
			me.handler(message.Partition, message.Offset, message.Value, string(message.Key))
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
