package kafka

import (
	"bitbucket.org/subiz/executor"
	"bitbucket.org/subiz/gocommon"
	cpb "bitbucket.org/subiz/header/common"
	"bitbucket.org/subiz/map"
	"bitbucket.org/subiz/squasher"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"reflect"
	"sync/atomic"
	"time"
)

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
	consumer   Consumer
	hs         map[string]handlerFunc
	term       uint64
	topic      string
	exec       *executor.Executor
	autocommit bool
	sqmap      cmap.Map
}

type Job struct {
	Message *sarama.ConsumerMessage
	Term    uint64
}

func NewHandlerFromCsm(csm Consumer, topic string, maxworkers, maxlag uint, autocommit bool) *Handler {
	h := &Handler{
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

func callHandler(handler map[string]handlerFunc, val []byte, par int32, offset int64) error {
	// examize val to get topic
	payload := &cpb.Empty{}
	if err := proto.Unmarshal(val, payload); err != nil {
		return err
	}

	pctx := payload.GetCtx()
	hf := handler[pctx.GetTopic()]
	if hf.paramType == nil {
		return fmt.Errorf("wrong handler for topic" + pctx.GetTopic())
	}

	// examize val to get data
	pptr := reflect.New(hf.paramType.Elem())
	pptr.Elem().Set(reflect.Zero(hf.paramType.Elem()))
	intef := pptr.Interface().(proto.Message)
	if err := proto.Unmarshal(val, intef); err != nil {
		return err
	}

	pctx.Offset, pctx.Partition = offset, par
	ctxval := reflect.ValueOf(common.ToGrpcCtx(pctx))

	hf.function.Call([]reflect.Value{ctxval, pptr})
	return nil
}

func convertToHanleFunc(handlers R) map[string]handlerFunc {
	rs := make(map[string]handlerFunc)
	for k, v := range handlers {
		f := reflect.ValueOf(v)
		ptype := f.Type().In(1)

		pptr := reflect.New(ptype.Elem())
		_, ok := pptr.Interface().(proto.Message)
		if !ok {
			panic("wrong handler for topic " + k.String() +
				". The second param should be type of proto.Message")
		}

		rs[k.String()] = handlerFunc{paramType: ptype, function: f}
	}
	return rs
}

func (h *Handler) Commit(partition int32, offset int64) {
	h.markSq(partition, offset)
}

func (h *Handler) handleJob(job executor.Job) {
	j := job.Data.(Job)
	mes := j.Message

	// ignore old term
	if atomic.LoadUint64(&h.term) != j.Term {
		return
	}
	if err := callHandler(h.hs, mes.Value, mes.Partition, mes.Offset); err != nil {
		common.LogErr(err)
		return
	}

	if h.autocommit {
		h.markSq(mes.Partition, mes.Offset)
	}
}

func (h *Handler) markSq(partition int32, offset int64) {
	term := atomic.LoadUint64(&h.term)
	sqi, ok := h.sqmap.Get(fmt.Sprintf("%d-%d", term, partition))
	if !ok {
		return
	}
	sq := sqi.(*squasher.Squasher)
	sq.Mark(offset)
}

func (h *Handler) commitloop(term uint64, par int32, offsetc <-chan int64) {
	changed := false
	for {
		if atomic.LoadUint64(&h.term) != term {
			return // term changed, our term is outdated
		}

		select {
		case offset := <-offsetc:
			h.consumer.MarkOffset(&sarama.ConsumerMessage{
				Topic:     h.topic,
				Offset:    offset,
				Partition: par,
			}, "")
			changed = true
		case <-time.After(1 * time.Second):
			if !changed {
				break
			}
			h.consumer.CommitOffsets()
			changed = false
		}
	}
}

func (h *Handler) createSqIfNotExist(par int32, offset int64) {
	term := atomic.LoadUint64(&h.term)
	_, ok := h.sqmap.Get(fmt.Sprintf("%d-%d", term, par))
	if ok {
		return
	}

	sq := squasher.NewSquasher(offset, 10000)
	h.sqmap.Set(fmt.Sprintf("%d-%d", term, par), sq)
	go h.commitloop(term, par, sq.Next())
}

func (h *Handler) Serve(handler R, cbs ...interface{}) {
	h.hs = convertToHanleFunc(handler)
	var nh func([]int32)
	if len(cbs) > 0 {
		nh = cbs[0].(func([]int32))
	}
	for {
		select {
		case msg, more := <-h.consumer.Messages():
			if !more {
				goto end
			}

			h.createSqIfNotExist(msg.Partition, msg.Offset)
			h.exec.AddJob(executor.Job{
				Key:  string(msg.Key),
				Data: Job{Term: h.term, Message: msg},
			})
		case ntf, more := <-h.consumer.Notifications():
			if !more {
				goto end
			}
			atomic.AddUint64(&h.term, 1)
			h.sqmap = cmap.New(100)

			if nh != nil {
				nh(ntf.Current[h.topic])
			}
		case err := <-h.consumer.Errors():
			common.LogErr(err)
		case <-EndSignal():
			goto end
		}
	}
end:
	err := h.consumer.Close()
	common.DieIf(err, -10, "unable to close consumer")
}

func newHandlerConsumer(brokers []string, topic, csg string) *cluster.Consumer {
	c := cluster.NewConfig()
	c.Consumer.MaxWaitTime = 10000 * time.Millisecond
	//c.Consumer.Offsets.Retention = 0
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Group.Session.Timeout = 20 * time.Second
	c.Group.Return.Notifications = true

	var err error
	var consumer *cluster.Consumer
	for {
		consumer, err = cluster.NewConsumer(brokers, csg, []string{topic}, c)
		if err != nil {
			common.Log(err, "will retry...")
			time.Sleep(3 * time.Second)
			continue
		}
		break
	}
	return consumer
}
