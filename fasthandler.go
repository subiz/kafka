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
	maxworkers uint
	topic      string
	handlers   map[string]func(*cpb.Context, []byte)
	rebalanceF func([]int32) error

	// exec is job balancer
	exec *executor.Executor
}

func (me *FastHandler) Setup(session sarama.ConsumerGroupSession) error {
	me.exec = executor.New(me.maxworkers, func(key string, p interface{}) {
		msg := p.(*sarama.ConsumerMessage)
		val, subtopic, pctx := msg.Value, "", &cpb.Context{}
		ks := strings.Split(key, "-")
		if len(ks) >= 2 {
			subtopic = ks[0]
		} else {
			payload := &cpb.Empty{}
			if err := proto.Unmarshal(val, payload); err == nil {
				if p := payload.GetCtx(); p != nil {
					pctx = p
				}
				subtopic = pctx.GetSubTopic()
			}
		}

		hf, ok := me.handlers[subtopic]
		if !ok || hf == nil {
			return
		}

		pctx.KafkaKey, pctx.KafkaOffset, pctx.KafkaPartition = key, msg.Offset, msg.Partition
		hf(pctx, val)
	})
	if me.rebalanceF != nil {
		return me.rebalanceF(session.Claims()[me.topic])
	}
	return nil
}

func (me *FastHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	if me.rebalanceF != nil {
		return me.rebalanceF(nil)
	}
	return nil
}

func (me *FastHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	par := claim.Partition()
	log.Println("Kafka Info #852459 start comsume partition ", par, " of topic ", claim.Topic(),
		" at ", claim.InitialOffset())
	initOffset := claim.InitialOffset()
	sq := squasher.NewSquasher(initOffset, int32(me.maxworkers*100*2)) // 1M
	ofsc := sq.Next()
	t := time.NewTicker(1 * time.Second)

	for {
		select {
		case o := <-ofsc:
			session.MarkOffset(me.topic, par, o, "")
		case <-t.C:
			ss := strings.Split(sq.GetStatus(), " .. ")
			if len(ss) == 3 && len(ss[0]) > 2 && len(ss[2]) > 2 {
				a, b, c := ss[0][1:], ss[1], ss[2][:len(ss[2])-1]
				if a != b || b != c {
					log.Println("Handle status ", par, sq.GetStatus())
				}
			}
		case x := <-session.Context().Done():
			log.Println("CANCELLED", claim.Partition(), x)
			return nil
		case msg, more := <-claim.Messages():
			if !more {
				return nil
			}

			if msg.Offset <= initOffset {
				continue
			}
			me.exec.Add(string(msg.Key), msg)
		}
	}
}

func NewFastHandler(brokers []string, consumergroup, topic string,
	handlers map[string]func(*cpb.Context, []byte),
	rebalanceF func([]int32) error) error {
	c := sarama.NewConfig()
	c.Version = sarama.V2_1_0_0
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	group, err := sarama.NewConsumerGroup(brokers, consumergroup, c)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case err := <-group.Errors():
				if err != nil {
					log.Printf("Kafka err #24525294: %s\n", err.Error())
				}
			}
		}
	}()

	handler := &FastHandler{topic: topic, handlers: handlers, rebalanceF: rebalanceF}
	for {
		err = group.Consume(context.Background(), []string{topic}, handler)
		if err != nil {
			return err
		}
	}
}
