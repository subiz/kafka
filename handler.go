package kafka

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/squasher"
)

// Handler is used to consumer kafka messages. Unlike default consumer, it
// supports routing using sub topic. Unlike FastHandler, user must commit
// message manually.
type Handler struct {
	// holds address of kafka brokers, eg: ['kafka-0:9092', 'kafka-1:9092']
	brokers []string

	// holds consumer group id
	consumergroup string

	// holds kafka topic
	topic string

	// kafka client id, for debugging
	ClientID string

	// holds a map of function which will be trigger on every kafka messages.
	// it maps subtopic to a function
	handlers map[string]func(*cpb.Context, []byte)

	// a callback function that will be trigger every time the group is rebalanced
	rebalanceF func([]int32)

	sqlock *sync.Mutex // protect sqmap
	sqmap  map[int32]*squasher.Squasher
}

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
	me.sqmap = nil
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
	ofsc := make(<-chan int64)

	handlers, topic := me.handlers, me.topic

	firstmessage := true
	for {
		select {
		case o := <-ofsc:
			// o+1 because we want consumer offset to point to the next message
			session.MarkOffset(topic, par, o+1, "")
		case msg, more := <-claim.Messages():
			if !more {
				log.Println("KAFKA FORCE EXIT AT PARTITION", claim.Partition())
				return nil
			}

			if firstmessage {
				firstmessage = false
				log.Println("Kafka Info #852459 start comsume partition", par, "of topic",
					claim.Topic(), "at", msg.Offset)
				me.sqlock.Lock()
				me.sqmap[par] = squasher.NewSquasher(msg.Offset, int32(10000)) // 1M
				ofsc = me.sqmap[par].Next()
				me.sqlock.Unlock()
			}

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
				pctx.KafkaKey, pctx.KafkaOffset, pctx.KafkaPartition = string(msg.Key), msg.Offset, msg.Partition
				hf(pctx, val)
			}
		}
	}
}

func (me *Handler) Commit(partition int32, offset int64) {
	me.sqlock.Lock()
	if sq := me.sqmap[partition]; sq != nil {
		sq.Mark(offset)
	}
	me.sqlock.Unlock()
}

// NewHandler creates a new FastHandler object
func NewSimpleHandler(brokers []string, consumergroup, topic string) *Handler {
	hostname, _ := os.Hostname()
	return &Handler{
		brokers:       brokers,
		consumergroup: consumergroup,
		topic:         topic,
		ClientID:      hostname,
		sqmap:         make(map[int32]*squasher.Squasher),
		sqlock:        &sync.Mutex{},
	}
}

// Serve listens messages from kafka and call matched handlers
func (me *Handler) Serve(handlers map[string]func(*cpb.Context, []byte),
	rebalanceF func([]int32)) {
	c := sarama.NewConfig()
	c.Version = sarama.V2_1_0_0
	c.ClientID = me.ClientID
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	group, err := sarama.NewConsumerGroup(me.brokers, me.consumergroup, c)
	if err != nil {
		panic(err)
	}

	me.handlers, me.rebalanceF = handlers, rebalanceF
	go func() {
		for err := range group.Errors() {
			log.Printf("Kafka err #24525294: %s\n", err.Error())
		}
	}()

	for {
		err = group.Consume(context.Background(), []string{me.topic}, me)
		if err != nil {
			log.Printf("Kafka err #222293: %s\nRetry in 2 secs\n", err.Error())
			time.Sleep(2 * time.Second)
		}
	}
}
