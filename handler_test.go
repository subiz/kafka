package kafka

import (
	"bitbucket.org/subiz/goutils/grpc"
	"bitbucket.org/subiz/header/account"
	cpb "bitbucket.org/subiz/header/common"
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"hash/crc32"
	"testing"
	"time"
)

var crc32q = crc32.MakeTable(0xD5828281)

type String struct {
	s string
}

func (s String) String() string { return s.s }

type ConsumerMock struct {
	mes    chan *sarama.ConsumerMessage
	not    chan *cluster.Notification
	commit chan *sarama.ConsumerMessage
	maxpar int32
}

func NewConsumerMock(par int32) *ConsumerMock {
	return &ConsumerMock{
		maxpar: par,
		mes:    make(chan *sarama.ConsumerMessage),
		commit: make(chan *sarama.ConsumerMessage),
		not:    make(chan *cluster.Notification),
	}
}

func (c *ConsumerMock) CommitChan() <-chan *sarama.ConsumerMessage {
	return c.commit
}

func (c *ConsumerMock) Publish(topic string, offset int64, key string, p proto.Message) {
	b, _ := proto.Marshal(p)

	par := int32(crc32.Checksum([]byte(key), crc32q)) % c.maxpar
	c.mes <- &sarama.ConsumerMessage{
		Partition: par,
		Topic:     topic,
		Key:       []byte(key),
		Offset:    offset,
		Value:     b,
	}
}

func (c *ConsumerMock) Notify(noti map[string][]int32) {
	c.not <- &cluster.Notification{Current: noti}
}

func (c *ConsumerMock) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	c.commit <- msg
}

func (c *ConsumerMock) CommitOffsets() error { return nil }

func (c *ConsumerMock) Messages() <-chan *sarama.ConsumerMessage {
	return c.mes
}

func (c *ConsumerMock) Notifications() <-chan *cluster.Notification {
	return c.not
}

func (c *ConsumerMock) Errors() <-chan error {
	return make(chan error, 0)
}

func (c *ConsumerMock) Close() error { return nil }

func createMsg(topic, name string) *account.Account {
	a := &account.Account{
		Ctx:  &cpb.Context{Topic: topic},
		Name: &name,
	}
	return a
}

func TestCommitManually(t *testing.T) {
	N, called := int64(400), int64(0)
	call, done := make(chan bool), make(chan bool)

	topic := "mot"
	csm := NewConsumerMock(4)
	go func() {
		for c := range csm.CommitChan() {
			if c.Offset == N-1 {
				<-call
				done <- true
			}
		}
	}()
	h := NewHandlerFromCsm(csm, topic, 100, 10, false)
	r := R{
		&String{"mot"}: func(ctx context.Context, p *account.Account) {
			called++
			go func(called int64) {
				ct := grpc.FromGrpcCtx(ctx)
				h.Commit(ct.GetTerm(), ct.GetPartition(), ct.GetOffset())
				time.Sleep(time.Duration(N - called))
			}(called)

			if called == N {
				go func() {
					call <- true
				}()
			}
		},
	}

	go h.Serve(r, func([]int32) {})

	csm.Notify(map[string][]int32{topic: []int32{0, 1, 2}})
	for i := int64(0); i < N; i++ {
		csm.Publish(topic, i, "a", createMsg(topic, fmt.Sprintf("%d", i)))
	}
	<-done
}

func TestCommit(t *testing.T) {
	N, called := int64(400), int64(0)
	call, done := make(chan bool), make(chan bool)

	topic := "mot"
	csm := NewConsumerMock(4)
	go func() {
		for c := range csm.CommitChan() {
			if c.Offset == N-1 {
				<-call
				done <- true
			}
		}
	}()
	h := NewHandlerFromCsm(csm, topic, 100, 10, true)
	r := R{
		&String{"mot"}: func(ctx context.Context, p *account.Account) {
			called++
			if called == N {
				go func() {
					call <- true
				}()
			}
		},
	}

	go h.Serve(r, func([]int32) {})

	csm.Notify(map[string][]int32{topic: []int32{0, 1, 2}})
	for i := int64(0); i < N; i++ {
		csm.Publish(topic, i, "a", createMsg(topic, fmt.Sprintf("%d", i)))
	}
	<-done
}

func TestUnwantedTopics(t *testing.T) {
	N, called := int64(400), int64(0)
	call, done := make(chan bool), make(chan bool)

	topic := "mot"
	csm := NewConsumerMock(4)
	go func() {
		for c := range csm.CommitChan() {
			if c.Offset == N-1 {
				<-call
				done <- true
			}
		}
	}()
	h := NewHandlerFromCsm(csm, topic, 100, 10, true)
	r := R{
		&String{topic}: func(ctx context.Context, p *account.Account) {
			called++
			if called == N {
				go func() {
					call <- true
				}()
			}
		},
	}

	go h.Serve(r, func([]int32) {})

	csm.Notify(map[string][]int32{topic: []int32{0, 1, 2}})
	for i := int64(0); i < N; i++ {
		if i%2 == 0 {
			csm.Publish(topic, i, "a", createMsg(topic, fmt.Sprintf("%d", i)))
		} else {
			csm.Publish(topic+"d", i, "a", createMsg(topic, fmt.Sprintf("%d", i)))
		}
	}
	<-done
}
