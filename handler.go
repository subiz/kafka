package kafka

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/paulbellamy/ratecounter"
	"github.com/subiz/header"
)

// var hostname string // search-n
type HandlerFunc func(topic string, partition int32, data []byte)

// Serve listens messages from kafka and call matched handlers
func Listen(consumerGroup, topic string, handleFunc HandlerFunc, addrs ...string) error {
	if len(addrs) == 0 {
		addrs = g_brokers
	}
	// hostname, _ = os.Hostname()

	if topic == "" {
		return header.E400(nil, header.E_invalid_account_id, "topic cannot be empty")
	}
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	//  config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	counter := ratecounter.NewRateCounter(1 * time.Minute)
	go func() {
		for {
			time.Sleep(120 * time.Second)
			log.Println("KAFKA RATE", topic, counter.Rate())
		}
	}()

	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		return err
	}

	pars, err := client.Partitions(topic)
	if err != nil {
		return err
	}
	nPartitions := len(pars)
	wg := &sync.WaitGroup{}

	cancels := []context.CancelFunc{}
	for i := 0; i < nPartitions; i++ {
		wg.Add(1)
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)

		con := &consumer{counter: counter, handler: handleFunc}
		client, err := sarama.NewConsumerGroup(addrs, consumerGroup, config)
		if err != nil {
			return err
		}

		go func(ctx context.Context, group sarama.ConsumerGroup, con *consumer) {
			defer wg.Done()
			for {
				// `Consume` should be called inside an infinite loop, when a
				// server-side rebalance happens, the consumer session will need to be
				// recreated to get the new claims
				if err := group.Consume(ctx, []string{topic}, con); err != nil {
					panic(err)
				}
				// check if context was cancelled, signaling that the consumer should stop
				if ctx.Err() != nil {
					return
				}
			}
		}(ctx, client, con)
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	for _, cancel := range cancels {
		cancel()
	}
	wg.Wait()
	return client.Close()
}

// Consumer represents a Sarama consumer group consumer
type consumer struct {
	handler HandlerFunc
	counter *ratecounter.RateCounter
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (*consumer) Setup(sarama.ConsumerGroupSession) error { return nil }

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (*consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (me *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			// log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			me.counter.Incr(1)
			me.handler(message.Topic, message.Partition, message.Value)
			session.MarkMessage(message, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
