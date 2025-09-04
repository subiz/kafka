package kafka

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/paulbellamy/ratecounter"
	"github.com/subiz/executor/v2"
	"github.com/subiz/log"
	"github.com/subiz/squasher/v2"
)

// var hostname string // search-n
type HandlerFunc func(partition int32, offset int64, data []byte, key string)
type HandlerFuncCtx func(ctx context.Context, partition int32, offset int64, data []byte, key string)

type PartitionHandlerFunc func(offset int64, data []byte)

type CommitOffset struct {
	Partition int32
	Offset    int64
}

func Consume(broker, consumerGroup, topic string, handleFunc HandlerFuncCtx) (*Consumer, error) {
	var consumer *Consumer
	var err error
	consumer, err = ConsumeAsync(broker, consumerGroup, topic, func(ctx context.Context, partition int32, offset int64, data []byte, key string) {
		handleFunc(ctx, partition, offset, data, key)
		consumer.Commit(partition, offset)
	})
	return consumer, err
}

type Consumer struct {
	topic      string
	commitchan chan CommitOffset
	closechan  chan bool
}

func (me *Consumer) Commit(partition int32, offset int64) {
	me.commitchan <- CommitOffset{Partition: partition, Offset: offset}
}

func (me *Consumer) Close() {
	me.closechan <- true
}

func (me *Consumer) CloseAsync() {
	select {
	case me.closechan <- true:
	default:
	}
}

// return max number of uncommit kafka messages for all partitions in a topic
func getConsumerGroupLag(client sarama.Client, admin sarama.ClusterAdmin, broker, consumerGroup, topic string) (int, error) {
	partitions, err := client.Partitions(topic)
	if err != nil {
		return 0, err
	}

	if len(partitions) == 0 {
		return 0, nil
	}

	topicParts := map[string][]int32{topic: partitions}
	groupOffsets, err := admin.ListConsumerGroupOffsets(consumerGroup, topicParts)
	if err != nil {
		return 0, err
	}

	maxLag := int64(0)
	for _, partition := range partitions {
		latestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Err("subiz", err, "msg", "failed to get latest offset", "topic", topic, "partition", partition)
			continue
		}

		block := groupOffsets.GetBlock(topic, partition)
		groupOffset := block.Offset

		var partitionLag int64
		// when a consumer group is first created, the offset is -1
		if groupOffset == -1 {
			// lag is the total number of messages in the partition
			oldestOffset, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				log.Err("subiz", err, "msg", "failed to get oldest offset", "topic", topic, "partition", partition)
				continue
			}
			partitionLag = latestOffset - oldestOffset
		} else {
			partitionLag = latestOffset - groupOffset
		}

		if partitionLag < 0 {
			partitionLag = 0
		}
		if partitionLag > maxLag {
			maxLag = partitionLag
		}
	}

	return int(maxLag), nil
}

func ConsumeAsync(broker, consumerGroup, topic string, handleFunc HandlerFuncCtx) (*Consumer, error) {
	mainconsumer := &Consumer{topic: topic}
	dead := false
	closechan := make(chan bool, 2)
	mainconsumer.closechan = closechan

	commitchan := make(chan CommitOffset, 1000)
	mainconsumer.commitchan = commitchan

	if topic == "" {
		return nil, log.EMissingId("topic")
	}
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	//  config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Admin.Timeout = 10 * time.Second

	pclient, err := sarama.NewClient([]string{broker}, config)
	if err != nil {
		return nil, err
	}
	partitions, err := pclient.Partitions(topic)
	if err != nil {
		pclient.Close()
		return nil, err
	}

	// Build map[topic][]partition for the admin call
	topicParts := map[string][]int32{topic: partitions}

	admin, err := sarama.NewClusterAdmin([]string{broker}, config)
	if err != nil {
		pclient.Close()
		return nil, err
	}

	NPartition := len(partitions)
	squashers := make([]*squasher.Squasher, NPartition)

	groupOffsets, err := admin.ListConsumerGroupOffsets(consumerGroup, topicParts)
	if err != nil {
		pclient.Close()
		admin.Close()
		return nil, err
	}

	for _, partition := range partitions {
		block := groupOffsets.GetBlock(topic, partition)

		// Get earliest (first available) offset
		earliest, _ := pclient.GetOffset(topic, partition, sarama.OffsetOldest)
		sq := squasher.NewSquasher()
		squashers[partition] = sq
		offset := block.Offset
		if offset < earliest {
			offset = earliest
		}
		// when the queue is empty
		// + this offset will be -1
		// + first message will be 0
		// when the queue is not empty
		// + this offset will be n
		// + first message will be n

		// offset will point the the next message will receive (super confuse)
		if offset < 0 { // except for the very first msg
			offset = 0
		}
		sq.Init(offset)
	}

	lastCommitOffsets := make([]int64, NPartition)
	commitOffsets := make([]int64, NPartition)
	counter := ratecounter.NewRateCounter(1 * time.Minute)

	lock := &sync.Mutex{}
	_slowTrackM := map[string]int64{}
	go func() {
		for !dead {
			time.Sleep(5 * time.Minute)
			lag, err := getConsumerGroupLag(pclient, admin, broker, consumerGroup, topic)
			if lag > 50_000 || err != nil {
				log.Track(context.Background(), "kafka-lag", "topic", topic, "consumer-group", consumerGroup, "lag", lag, "err", err)
			}
			now := time.Now().UnixMilli()
			slowM := map[string]int64{}
			lock.Lock()
			slowTrackM := map[string]int64{}
			for k, created := range _slowTrackM {
				if now-created > 300_000 {
					slowM[k] = now - created
					continue
				}
				slowTrackM[k] = created
			}
			_slowTrackM = slowTrackM
			lock.Unlock()
			for k, dur := range slowM {
				log.Track(context.Background(), "slow-kafka", "topic", topic, "consumer-group", consumerGroup, "partition.offset", k, "sec", dur/1000)
			}
		}
	}()

	queue := executor.New(func(key string, payloads []any) {
		if dead {
			return
		}

		for _, payload := range payloads {
			message := payload.(*sarama.ConsumerMessage)
			sq := squashers[int(message.Partition)]
			counter.Incr(1)

			// already consumed
			if sq.Check(message.Offset) {
				continue
			}
			now := time.Now().UnixMilli()
			pk := strconv.Itoa(int(message.Partition)) + "." + strconv.Itoa(int(message.Offset))
			lock.Lock()
			_slowTrackM[pk] = now
			lock.Unlock()
			handleFunc(context.Background(), message.Partition, message.Offset, message.Value, key)
			lock.Lock()
			delete(_slowTrackM, pk)
			lock.Unlock()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	con := newConsumer2(queue.Add)

	client, err := sarama.NewConsumerGroup([]string{broker}, consumerGroup, config)
	if err != nil {
		cancel()
		pclient.Close()
		admin.Close()
		return nil, err
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

	go func() {
		for co := range commitchan {
			if dead {
				return
			}

			sq := squashers[int(co.Partition)]
			commitoffset := sq.Mark(co.Offset)
			lock.Lock()
			commitOffsets[int(co.Partition)] = commitoffset
			lock.Unlock()
		}
	}()

	go func() {
		for !dead {
			time.Sleep(3 * time.Second)
			if con.session == nil {
				continue
			}
			lock.Lock()
			mycommitOffsets := commitOffsets
			commitOffsets = make([]int64, NPartition)
			lock.Unlock()

			log.Info("subiz", "KAFKARATE", topic, counter.Rate(), "msg/sec")
			for partition, offset := range mycommitOffsets {
				if lastCommitOffsets[partition] >= offset {
					continue
				}
				// fmt.Println("MARKED", consumerGroup, topic, int32(partition), offset)
				con.MarkOffset(topic, int32(partition), offset)
				lastCommitOffsets[partition] = offset
			}
		}
	}()

	go func() {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-sigterm:
			dead = true
			cancel()
			client.Close()
			admin.Close()
			pclient.Close()

			panic("FORCEEXIT")
		case <-closechan:
			dead = true
			cancel()
			admin.Close()
			pclient.Close()
			client.Close()
		}
	}()
	return mainconsumer, nil
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
			me.handler(string(message.Key), message)
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
