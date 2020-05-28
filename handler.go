package kafka

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/paulbellamy/ratecounter"
	"github.com/subiz/executor"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/squasher"
)

// Handler is used to consumer kafka messages. Unlike default consumer, it
// supports routing using sub topic, parallel execution and auto commit.
type Handler struct {
	EnableLog bool

	// holds address of kafka brokers, eg: ['kafka-0:9092', 'kafka-1:9092']
	brokers []string

	// holds consumer group id
	consumergroup string

	// holds kafka topic
	topic string

	// kafka client id, for debugging
	ClientID string

	// number of concurrent workers per kafka partition
	maxworkers uint

	// holds a map of function which will be trigger on every kafka messages.
	// it maps subtopic to a function
	handlers H

	// a callback function that will be trigger every time the group is rebalanced
	rebalanceF func([]int32)

	group sarama.ConsumerGroup

	// commit kafka event automatically when handle returned
	auto_commit bool

	sqlock *sync.Mutex // protect sqmap
	sqmap  map[int32]*squasher.Squasher

	counter *ratecounter.RateCounter
}

// syntax suggar for define a map of event hander, with the key is the topic
type H map[string]func(*cpb.Context, []byte)

// Setup implements sarama.ConsumerGroupHandler.Setup, it is run at the
// beginning of a new session, before ConsumeClaim.
func (me *Handler) Setup(session sarama.ConsumerGroupSession) error {
	if me.rebalanceF != nil {
		me.rebalanceF(session.Claims()[me.topic])
	}
	return nil
}

// Cleanup impements sarama.ConsumerGroupHandler.Cleanup, it is run at the
// end of a session, once all ConsumeClaim goroutines have exited but
// before the offsets are committed for the very last time.
func (me *Handler) Cleanup(_ sarama.ConsumerGroupSession) error {
	me.sqlock.Lock()
	me.sqmap = make(map[int32]*squasher.Squasher)
	me.sqlock.Unlock()
	if me.rebalanceF != nil {
		me.rebalanceF(nil)
	}
	return nil
}

// ConsumeClaim implements sarama.ConsumerGroupHandler.ConsumerClaim, it must
// start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (me *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	par := claim.Partition()
	var sq *squasher.Squasher
	ofsc := make(<-chan int64)

	handlers, topic := me.handlers, me.topic
	exec := executor.New(me.maxworkers, func(key string, p interface{}) {
		msg := p.(*sarama.ConsumerMessage)
		val, pctx := msg.Value, &cpb.Context{}
		var empty cpb.Empty
		if err := proto.Unmarshal(val, &empty); err == nil {
			if empty.GetCtx() != nil {
				pctx = empty.GetCtx()
			}
		}
		subtopic := pctx.GetSubTopic()
		hf, ok := handlers[subtopic]
		if !ok && subtopic != "" { // subtopic not found, fallback to default handler
			hf = handlers[""]
		}
		if hf != nil {
			pctx.KafkaKey, pctx.KafkaOffset, pctx.KafkaPartition = key, msg.Offset, msg.Partition
			hf(pctx, val)
		}
		if me.auto_commit {
			sq.Mark(msg.Offset)
		}
	})

	defer func() {
		exec.Wait()
		exec.Stop() // clean up resource
	}()

	firstmessage := true
	t := time.NewTicker(1 * time.Second) // used to check slow consumer
	for {
		select {
		case o := <-ofsc:
			// o+1 because we want consumer offset to point to the next message
			session.MarkOffset(topic, par, o+1, "")
		case <-t.C:
			if sq == nil {
				continue
			}

			ss := strings.Split(sq.GetStatus(), " .. ")
			if len(ss) == 3 && len(ss[0]) > 2 && len(ss[2]) > 2 {
				a, b, c := ss[0][1:], ss[1], ss[2][:len(ss[2])-1]
				if a != b || b != c {
					log.Println("Handle status ", par, sq.GetStatus())
				}
			}
		case msg, more := <-claim.Messages():
			if !more {
				log.Println("KAFKA FORCE EXIT AT PARTITION", claim.Partition())
				return nil
			}

			me.counter.Incr(1)

			if firstmessage {
				firstmessage = false
				log.Println("Kafka Info #852459 start comsume partition", par, "of topic",
					claim.Topic(), "at", msg.Offset)
				sq = squasher.NewSquasher(msg.Offset, int32(me.maxworkers*100000)) // 800k item each worker
				ofsc = sq.Next()
				me.sqlock.Lock()
				me.sqmap[par] = sq
				me.sqlock.Unlock()
			}
			exec.Add(string(msg.Key), msg)
		}
	}
}

// NewHandler creates a new Handler object
func NewHandler(brokers []string, consumergroup, topic string, autocommit bool) *Handler {
	hostname, _ := os.Hostname()
	return &Handler{
		brokers:       brokers,
		consumergroup: consumergroup,
		maxworkers:    5,
		topic:         topic,
		ClientID:      hostname,
		sqlock:        &sync.Mutex{},
		sqmap:         make(map[int32]*squasher.Squasher, 0),
		auto_commit:   autocommit,
	}
}

// Serve listens messages from kafka and call matched handlers
func (me *Handler) Serve(handlers H, rebalanceF func([]int32)) {
	c := sarama.NewConfig()
	c.Version = sarama.V2_1_0_0
	c.ClientID = me.ClientID
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	var err error
	me.group, err = sarama.NewConsumerGroup(me.brokers, me.consumergroup, c)
	if err != nil {
		panic(err)
	}

	me.counter = ratecounter.NewRateCounter(1 * time.Second)

	me.handlers, me.rebalanceF = handlers, rebalanceF
	go func() {
		for err := range me.group.Errors() {
			log.Printf("Kafka err #24525294: %s\n", err.Error())
		}
	}()

	if me.EnableLog {
		go func() {
			for {
				log.Println("KAFKA RATE", me.topic, me.counter.Rate())
				time.Sleep(4 * time.Second)
			}
		}()
	}

	for {
		err = me.group.Consume(context.Background(), []string{me.topic}, me)
		if err != nil {
			log.Printf("Kafka err #222293: %s\nRetry in 2 secs\n", err.Error())
			time.Sleep(2 * time.Second)
		}
	}
}

// Close stops the handler and detaches any running sessions
func (me *Handler) Close() error { return me.group.Close() }

// Commit marks kafka event as processed so it won't restream next time server
// is started
func (me *Handler) Commit(partition int32, offset int64) {
	me.sqlock.Lock()
	if sq := me.sqmap[partition]; sq != nil {
		sq.Mark(offset)
	}
	me.sqlock.Unlock()
}
