package kafka

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/subiz/executor"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/squasher"
)

// FastHandler is used to consumer kafka messages. Unlike default consumer, it
// supports routing using sub topic, parallel execution and auto commit.
type FastHandler struct {
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
	handlers map[string]func(*cpb.Context, []byte)

	// a callback function that will be trigger every time the group is rebalanced
	rebalanceF func([]int32)

	group sarama.ConsumerGroup
}

// Setup implements sarama.ConsumerGroupHandler.Setup, it is run at the
// beginning of a new session, before ConsumeClaim.
func (me *FastHandler) Setup(session sarama.ConsumerGroupSession) error {
	if me.rebalanceF != nil {
		me.rebalanceF(session.Claims()[me.topic])
	}
	return nil
}

// Cleanup impements sarama.ConsumerGroupHandler.Cleanup, it is run at the
// end of a session, once all ConsumeClaim goroutines have exited but
// before the offsets are committed for the very last time.
func (me *FastHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	if me.rebalanceF != nil {
		me.rebalanceF(nil)
	}
	return nil
}

// ConsumeClaim implements sarama.ConsumerGroupHandler.ConsumerClaim, it must
// start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (me *FastHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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
		sq.Mark(msg.Offset)
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

			if firstmessage {
				firstmessage = false
				log.Println("Kafka Info #852459 start comsume partition", par, "of topic",
					claim.Topic(), "at", msg.Offset)
				sq = squasher.NewSquasher(msg.Offset, int32(me.maxworkers*100*2)) // 1M
				ofsc = sq.Next()
			}
			exec.Add(string(msg.Key), msg)
		}
	}
}

// NewHandler creates a new FastHandler object
func NewHandler(brokers []string, consumergroup, topic string) *FastHandler {
	hostname, _ := os.Hostname()
	return &FastHandler{
		brokers:       brokers,
		consumergroup: consumergroup,
		maxworkers:    50,
		topic:         topic,
		ClientID:      hostname,
	}
}

// Serve listens messages from kafka and call matched handlers
func (me *FastHandler) Serve(handlers map[string]func(*cpb.Context, []byte),
	rebalanceF func([]int32)) {
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

	me.handlers, me.rebalanceF = handlers, rebalanceF
	go func() {
		for err := range me.group.Errors() {
			log.Printf("Kafka err #24525294: %s\n", err.Error())
		}
	}()

	for {
		err = me.group.Consume(context.Background(), []string{me.topic}, me)
		if err != nil {
			log.Printf("Kafka err #222293: %s\nRetry in 2 secs\n", err.Error())
			time.Sleep(2 * time.Second)
		}
	}
}

func (me *FastHandler) Close() error {
	return me.group.Close()
}
