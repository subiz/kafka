package kafka

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/subiz/executor"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/squasher"
)

type FastHandler struct {
	brokers       []string
	consumergroup string
	topic         string

	group sarama.ConsumerGroup

	maxworkers uint // maxworkers per partition
	handlers   map[string]func(*cpb.Context, []byte)
	rebalanceF func([]int32)

	stopped bool
}

func (me *FastHandler) Setup(session sarama.ConsumerGroupSession) error {
	if me.rebalanceF != nil {
		me.rebalanceF(session.Claims()[me.topic])
	}
	return nil
}

func (me *FastHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	if me.rebalanceF != nil {
		me.rebalanceF(nil)
	}
	return nil
}

func (me *FastHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	par := claim.Partition()
	var sq *squasher.Squasher
	ofsc := make(<-chan int64)
	t := time.NewTicker(1 * time.Second)

	handlers, topic := me.handlers, me.topic
	exec := executor.New(me.maxworkers, func(key string, p interface{}) {
		msg := p.(*sarama.ConsumerMessage)
		val, pctx := msg.Value, &cpb.Context{}
		// ks := strings.Split(key, "")
		//if len(ks) >= 2 {
		//subtopic = ks[0]
		//} else {
		var empty cpb.Empty
		if err := proto.Unmarshal(val, &empty); err == nil {
			pctx = empty.GetCtx()
		}
		//}
		subtopic := pctx.GetSubTopic()
		hf, ok := handlers[subtopic]
		if !ok && subtopic != "" { // subtopic not found, fallback to default handler
			subtopic = ""
			hf = handlers[subtopic]
		}
		if hf != nil {
			pctx.KafkaKey, pctx.KafkaOffset, pctx.KafkaPartition = key, msg.Offset, msg.Partition
			hf(pctx, val)
		}
		sq.Mark(msg.Offset)
	})

	defer exec.Wait()

	initOffset := int64(-1)
	for {
		select {
		case o := <-ofsc:
			session.MarkOffset(topic, par, o, "")
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
		case <-session.Context().Done():
			log.Println("KAFKA FORCE EXIT AT PARTITION", claim.Partition())
			return nil
		case msg, more := <-claim.Messages():
			if !more {
				return nil
			}

			if initOffset < 0 { // just call once
				initOffset = msg.Offset
				log.Println("Kafka Info #852459 start comsume partition", par, "of topic",
					claim.Topic(), "at", initOffset)
				sq = squasher.NewSquasher(initOffset, int32(me.maxworkers*100*2)) // 1M
				ofsc = sq.Next()
			}
			exec.Add(string(msg.Key), msg)
		}
	}
}

// NewHandler creates a new Handler object
func NewHandler(brokers []string, consumergroup, topic string) *FastHandler {
	return &FastHandler{
		brokers:       brokers,
		consumergroup: consumergroup,
		maxworkers:    50,
		topic:         topic,
	}
}

// Stop stops listening for new kafka message
func (me *FastHandler) Stop() {
	if me.group != nil && !me.stopped {
		me.stopped = true
		me.group.Close()
	}
}

func (me *FastHandler) Serve(handlers map[string]func(*cpb.Context, []byte),
	rebalanceF func([]int32)) {
	c := sarama.NewConfig()
	c.Version = sarama.V2_1_0_0
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

	me.stopped = false
	for !me.stopped {
		err = group.Consume(context.Background(), []string{me.topic}, me)
		if err != nil {
			log.Printf("Kafka err #222293: %s\nRetry in 2 secs\n", err.Error())
			time.Sleep(2 * time.Second)
		}
	}
	group.Close()
}
