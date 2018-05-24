package kafka

import (
	"bitbucket.org/subiz/executor"
	"bitbucket.org/subiz/gocommon"
	cpb "bitbucket.org/subiz/header/common"
	"bitbucket.org/subiz/squasher"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"reflect"
	"sync"
	"time"
)

var notfounderr = fmt.Errorf("handler_not_found")

type handlerFunc struct {
	paramType reflect.Type
	function  reflect.Value
}

type Consumer interface {
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
	CommitOffsets() error
	Messages() <-chan *sarama.ConsumerMessage
	Notifications() <-chan *cluster.Notification
	Errors() <-chan error
	Close() error
}

type Handler struct {
	*sync.RWMutex
	// topic is kafka topic
	topic string

	// consumer is kafka consumer
	consumer Consumer

	// hs map of topic -> func
	hs map[string]handlerFunc

	// term is  current kafka term, it alway increase
	// and should equal number of kafka rebalances
	term uint64

	// exec is job balancer
	exec *executor.Executor

	// if set, commit automatically after handler func returned
	autocommit bool

	// sqmap is squasher per partition
	sqmap map[int32]*squasher.Squasher
}

type Job struct {
	*sarama.ConsumerMessage
	Term uint64
}

func NewHandlerFromCsm(csm Consumer, topic string, maxworkers, maxlag uint, autocommit bool) *Handler {
	h := &Handler{
		RWMutex:    &sync.RWMutex{},
		topic:      topic,
		autocommit: autocommit,
		consumer:   csm,
	}
	h.exec = executor.NewExecutor(maxworkers, maxlag, h.handleJob)
	return h
}

func NewHandler(brokers []string, csg, topic string, maxworkers, maxlag uint, autocommit bool) *Handler {
	csm := newHandlerConsumer(brokers, topic, csg)
	return NewHandlerFromCsm(csm, topic, maxworkers, maxlag, autocommit)
}

func callHandler(handler map[string]handlerFunc, val []byte, term uint64, par int32, offset int64) error {
	// examize val to get topic
	payload := &cpb.Empty{}
	topic, pctx := "", &cpb.Context{}
	if err := proto.Unmarshal(val, payload); err == nil {
		if p := payload.GetCtx(); p != nil {
			pctx = p
		}
		topic = pctx.GetTopic()
	} else {
		common.Logf("invalid %s:%d[%d] %v", topic, par, offset, val)
	}

	hf, ok := handler[topic]
	if !ok || hf.paramType == nil {
		return notfounderr
	}

	// examize val to get data
	pptr := reflect.New(hf.paramType)
	intef := pptr.Interface().(proto.Message)
	if err := proto.Unmarshal(val, intef); err != nil {
		common.Logf("router topic %s:%d[%d] %v", topic, par, offset, val)
		return err
	}

	pctx.Term, pctx.Offset, pctx.Partition = term, offset, par
	ctxval := reflect.ValueOf(common.ToGrpcCtx(pctx))

	hf.function.Call([]reflect.Value{ctxval, pptr})
	return nil
}

func convertToHandleFunc(handlers R) map[string]handlerFunc {
	rs := make(map[string]handlerFunc)
	for k, v := range handlers {
		f := reflect.ValueOf(v)
		ptype := f.Type().In(1).Elem()

		pptr := reflect.New(ptype)
		if _, ok := pptr.Interface().(proto.Message); !ok {
			panic("wrong handler for topic " + k.String() +
				". The second param should be type of proto.Message")
		}
		ks := ""
		if k != nil {
			ks = k.String()
		}
		rs[ks] = handlerFunc{paramType: ptype, function: f}
	}
	return rs
}

// do not call commit in nh function, it will cause deadlock
func (h *Handler) Commit(term uint64, partition int32, offset int64) {
	h.RLock()
	if h.term != term {
		h.RUnlock()
		return
	}
	sq := h.sqmap[partition]
	h.RUnlock()

	if sq != nil {
		sq.Mark(offset)
	}
}

func (h *Handler) handleJob(job executor.Job) {
	mes := job.Data.(Job)
	h.Lock()
	if h.term != mes.Term {
		h.Unlock()
		return
	}

	sq := h.createSqIfNotExist(mes.Partition, mes.Offset)
	h.Unlock()
	err := callHandler(h.hs, mes.Value, mes.Term, mes.Partition, mes.Offset)
	if err != nil && err != notfounderr {
		common.Logf("topic %s:%d[%d]", mes.Topic, mes.Partition, mes.Offset)
		common.LogErr(err)
	}

	if err != nil || h.autocommit {
		sq.Mark(mes.Offset)
	}
}

func (h *Handler) GetTerm() uint64 { return h.term }

func (h *Handler) commitloop(term uint64, par int32, ofsc <-chan int64) {
	changed, t := false, time.NewTicker(1*time.Second)
	for {
		select {
		case o := <-ofsc:
			h.RLock()
			if h.term != term {
				h.RUnlock()
				return
			}
			m := sarama.ConsumerMessage{Topic: h.topic, Offset: o, Partition: par}
			h.consumer.MarkOffset(&m, "")
			changed = true
			h.RUnlock()
		case <-t.C:
			h.RLock()
			if h.term != term {
				h.RUnlock()
				return
			}
			if sq := h.sqmap[par]; sq != nil {
				fmt.Println("Handle status ", h.term, par, sq.GetStatus())
			}

			if changed {
				h.consumer.CommitOffsets()
				changed = false
			}
			h.RUnlock()
		}
	}
}

func (h *Handler) createSqIfNotExist(par int32, offset int64) *squasher.Squasher {
	if sq := h.sqmap[par]; sq != nil {
		return sq
	}

	sq := squasher.NewSquasher(offset, 10000)
	h.sqmap[par] = sq
	go h.commitloop(h.term, par, sq.Next())
	return sq
}

func (h *Handler) Serve(handler R, nh func([]int32)) error {
	h.hs = convertToHandleFunc(handler)
loop:
	for {
		select {
		case msg, more := <-h.consumer.Messages():
			if !more || msg == nil {
				break loop
			}
			j := executor.Job{Key: string(msg.Key), Data: Job{msg, h.term}}
			h.exec.AddJob(j)
		case ntf := <-h.consumer.Notifications():
			if ntf == nil {
				break loop
			}
			h.Lock()
			h.term++
			h.sqmap = make(map[int32]*squasher.Squasher)
			func() {
				defer func() { recover() }()
				nh(ntf.Current[h.topic])
			}()
			h.Unlock()
		case err := <-h.consumer.Errors():
			common.LogErr(err)
		case <-EndSignal():
			break loop
		}
	}
	return h.consumer.Close()
}

func newHandlerConsumer(brokers []string, topic, csg string) *cluster.Consumer {
	c := cluster.NewConfig()
	c.Consumer.MaxWaitTime = 10000 * time.Millisecond
	//c.Consumer.Offsets.Retention = 0
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Group.Session.Timeout = 20 * time.Second
	c.Group.Return.Notifications = true

	for {
		csm, err := cluster.NewConsumer(brokers, csg, []string{topic}, c)
		if err == nil {
			return csm
		}
		common.Log(err, "will retry...")
		time.Sleep(3 * time.Second)
	}
}
