package kafka

import (
	"context"
	"encoding/hex"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/paulbellamy/ratecounter"
	"github.com/subiz/executor/v2"
	"github.com/subiz/header"
	"github.com/subiz/log"
	"github.com/subiz/squasher/v2"
)

// timeout 5 min
var HandlerTimeout = 5 * time.Minute

type CommitOffset struct {
	Partition int32
	Offset    int64
}

func Consume(broker, consumerGroup, topic string, handleFunc HandlerFuncCtx, closechan chan bool) error {
	commitchan := make(chan CommitOffset, 20)
	return ConsumeAsync(broker, consumerGroup, topic, func(ctx context.Context, partition int32, offset int64, data []byte, key string) {
		handleFunc(ctx, partition, offset, data, key)
		commitchan <- CommitOffset{Partition: partition, Offset: offset}
	}, closechan, commitchan)
}

func ConsumeAsync(broker, consumerGroup, topic string, handleFunc HandlerFuncCtx, closechan chan bool, commitchan chan CommitOffset) error {
	dead := false

	if topic == "" {
		return log.EMissingId("topic")
	}
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	//  config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// 1. find number of partitions
	pclient, err := sarama.NewClient([]string{broker}, config)
	if err != nil {
		return err
	}
	partitions, err := pclient.Partitions(topic)
	if err != nil {
		return err
	}
	pclient.Close()

	var NPartition = len(partitions)
	squashers := make([]*squasher.Squasher, NPartition)
	lastCommitOffsets := make([]int64, NPartition)
	commitOffsets := make([]int64, NPartition)
	counter := ratecounter.NewRateCounter(1 * time.Minute)

	// 2. connect to the marker
	conn := header.DialGrpc("marker-0.marker:17695", header.WithShardRedirect())
	marker := header.NewMarkerClient(conn)

	lock := &sync.Mutex{}
	markBuffers := make([][]int64, NPartition)

	queue := executor.New(func(key string, payload any) {
		if dead {
			return
		}
		message := payload.(*sarama.ConsumerMessage)
		sq := squashers[int(message.Partition)]
		counter.Incr(1)

		// already consumed
		if sq.Check(message.Offset) {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), HandlerTimeout)
		defer cancel() // Always cancel to release resources

		donec := make(chan bool, 1)
		go func() {
			handleFunc(ctx, message.Partition, message.Offset, message.Value, key)
			donec <- true
		}()
		select {
		case <-donec:
		case <-time.After(HandlerTimeout):
			hexStr := hex.EncodeToString(message.Value)
			log.Info("subiz", "KAFKATIMEOUT", topic, message.Partition, message.Offset, hexStr, key)
			log.Track(ctx, "kafka_timeout", "topic", topic, "parittion", message.Partition, "offset", message.Offset, "data", hexStr, "key", key)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	con := newConsumer2(func(key string, data any) {
		message := data.(*sarama.ConsumerMessage)
		sq := squashers[int(message.Partition)]
		if sq == nil {
			sq = squasher.NewSquasher()
			squashers[message.Partition] = sq
			sq.Init(message.Offset)
		}
		queue.Add(key, data)
	})
	client, err := sarama.NewConsumerGroup([]string{broker}, consumerGroup, config)
	if err != nil {
		cancel()
		return err
	}

	go func() {
		for !dead {
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

	for i := range NPartition {
		state, err := marker.ListMarkers(context.Background(), &header.MarkRequest{Topic: topic, Partition: int32(i)})
		if err != nil {
			log.Err("subiz", err)
		}
		for _, offset := range state.GetOffsets() {
			squashers[i].Mark(offset)
		}
	}

	go func() {
		for co := range commitchan {
			if dead {
				return
			}

			sq := squashers[int(co.Partition)]
			commitoffset := sq.Mark(co.Offset)
			lock.Lock()
			markBuffer := markBuffers[int(co.Partition)]
			markBuffer = append(markBuffer, co.Offset)
			commitOffsets[int(co.Partition)] = commitoffset
			lock.Unlock()
		}
	}()

	go func() {
		for {
			time.Sleep(5 * time.Second)
			if dead {
				return
			}

			lock.Lock()
			mycommitOffsets := commitOffsets
			commitOffsets = make([]int64, NPartition)
			lock.Unlock()

			log.Info("subiz", "KAFKA RATE", topic, counter.Rate())
			for partition, offset := range mycommitOffsets {
				// mark
				lock.Lock()
				markBuffer := markBuffers[partition]
				markBuffers[partition] = []int64{}
				lock.Unlock()

				marker.Mark(context.Background(), &header.MarkRequest{Topic: topic, Partition: int32(partition), Offsets: markBuffer})
				if lastCommitOffsets[partition] == offset {
					continue
				}

				con.MarkOffset(topic, int32(partition), offset)
				lastCommitOffsets[partition] = offset

				marker.Commit(context.Background(), &header.CommitRequest{Topic: topic, Partition: int32(partition), Offset: offset})
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigterm:
	case <-closechan:
	}
	dead = true
	cancel()
	return client.Close()
}

// Consumer represents a Sarama consumer group consumer
type consumer2 struct {
	handler func(key string, value any)
	session sarama.ConsumerGroupSession
}

func newConsumer2(handler func(key string, value any)) *consumer2 {
	return &consumer2{handler: handler}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (me *consumer2) Setup(session sarama.ConsumerGroupSession) error {
	me.session = session
	return nil
}

func (me *consumer2) MarkOffset(topic string, partition int32, offset int64) {
	me.session.MarkOffset(topic, partition, offset+1, "")
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (*consumer2) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (me *consumer2) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	for {
		select {
		case message, more := <-claim.Messages():
			if !more {
				return nil
			}
			if message != nil {
				me.handler(string(message.Key), message)
			}
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
