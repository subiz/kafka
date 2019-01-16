package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/subiz/squasher"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"
)

const MAP = "map"
const REDUCE = "reduce"
const BEGIN = "begin"
const GLOBALMAP = "global_map"

type Package struct {
	key  string
	data interface{}
}

type Phase struct {
	funcType string
	inchan   <-chan Package
	outchan  chan<- Package
}

type MapReducer struct {
	handler *Handler
	phases  []Phase
}

type Reducer interface {
	Map(f func(interface{}) string) Mapper
	GlobalMap(brokers []string, topic, csm string, f func(interface{}) string) Mapper
}

type Mapper interface {
	Reduce(f func(string, []Package) interface{}) Reducer
}

func NewMapReducer(csg, topic string) *MapReducer {
	me := &MapReducer{}
	//	me.handler = NewHandler(brokers, csg, topic, 1000, 100, false)
	return me
}

func publish(producer sarama.SyncProducer, topic string, data interface{}, key string) {
	msg := prepareMsg(topic, data, -1, key)
	if msg == nil {
		log.Println("no publish")
		return
	}

	for {
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			break
		}

		d := fmt.Sprintf("%v", data)
		if len(d) > 5000 {
			d = d[len(d)-4000:]
		}
		log.Printf("ERR SDFLF35LL unable to publish message, topic: %s, key %s, data: %s %v\n", topic, key, d, err)

		if err.Error() == sarama.ErrInvalidPartition.Error() ||
			err.Error() == sarama.ErrMessageSizeTooLarge.Error() {
			break
		}

		log.Println("retrying after 5sec")
		time.Sleep(5 * time.Second)
	}
}

func (me *MapReducer) GlobalMap(brokers []string, topic, csm string, f func(interface{}) string) Mapper {
	// take the last in
	// TODO: check err
	lastphase := me.phases[len(me.phases)-1]
	outc := make(chan<- Package)

	publisher := newHashedSyncPublisher(brokers)
	for pkg := range lastphase.inchan {
		key := f(pkg.data)
		publish(publisher, topic, pkg.data, key)
	}
	me.phases = append(me.phases, Phase{funcType: GLOBALMAP, outchan: outc})

	go me.receive(brokers, topic, csm, outc)
	return me
}

/*
	go func() {
		for {
			select {
			case pkgs := <-lastphase.reduce_inchan:

				data := make([]interface{}, 0)
				key := ""
				for _, pkg := range pkgs {
					key = pkg.key
					data = append(data, pkg.data)
				}
				out := f(key, data)
				outc <- Package{key: key, data: out}
			}
		}
	}()

*/
// Map registries a map function
func (me *MapReducer) Map(f func(interface{}) string) Mapper {
	lastphase := me.phases[len(me.phases)-1]
	outc := make(chan Package)

	for pkg := range lastphase.inchan {
		key := f(pkg.data)
		outc <- Package{key: key, data: pkg.data}
	}
	me.phases = append(me.phases, Phase{funcType: GLOBALMAP, outchan: outc})
	return me
}

func (me *MapReducer) Run() {

}

// Reduce registries a map function
func (me *MapReducer) Reduce(f func(string, []Package) interface{}) Reducer {
	// take the last in
	lastphase := me.phases[len(me.phases)-1]
	outc := make(chan Package)

	ticker := time.NewTicker(10 * time.Second)
	bigc := make(chan bool)

	m := make(map[string][]Package)
	bulkcount := 0
	clear := func() {
		for key, pkgs := range m {
			data := f(key, pkgs)
			outc <- Package{key: key, data: data}
			delete(m, key)
		}
		bulkcount = 0
	}

	go func() {
		for {
			select {
			case pkg := <-lastphase.inchan:
				pkgs := m[pkg.key]
				pkgs = append(pkgs, pkg)
				m[pkg.key] = pkgs

				bulkcount++
				if bulkcount > 1000 {
					clear()
				}
			case <-bigc:
				clear()
			case <-ticker.C:
				clear()
			}
		}
	}()

	me.phases = append(me.phases, Phase{funcType: REDUCE, outchan: outc})
	return me
}

func commitloop(csm *cluster.Consumer, topic string, par int32, sq *squasher.Squasher) {
	changed, t := false, time.NewTicker(1*time.Second)
	for {
		select {
		case o, more := <-sq.Next():
			if !more {
				return
			}

			changed = true
			m := sarama.ConsumerMessage{Topic: topic, Offset: o, Partition: par}
			csm.MarkOffset(&m, "")
		case <-t.C:

			ss := strings.Split(sq.GetStatus(), " .. ")
			if len(ss) == 3 && len(ss[0]) > 2 && len(ss[2]) > 2 {
				a, b, c := ss[0][1:], ss[1], ss[2][:len(ss[2])-1]
				if a != b || b != c {
					fmt.Println("Handle status ", par, sq.GetStatus())
				}
			}
			if changed {
				csm.CommitOffsets()
				changed = false
			}
		}
	}
}

func connectKafka(brokers []string, topic, csg string) (*cluster.Client, *cluster.Consumer) {
	c := cluster.NewConfig()
	c.Consumer.MaxWaitTime = 10000 * time.Millisecond
	//c.Consumer.Offsets.Retention = 0
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Group.Session.Timeout = 20 * time.Second
	c.Group.Return.Notifications = true

	client, err := cluster.NewClient(brokers, c)
	if err != nil {
		panic(err)
	}

	consumer, err := cluster.NewConsumerFromClient(client, csg, []string{topic})
	if err != nil {
		panic(err)
	}

	return client, consumer
}

// fetchInitialOffsets requests from kafka all offsets (a.k.a latest committed offsets)
// which a consumer of group csg will start streamming from.
// This function takes a connected kafka client, topic, consumer group ID and list
// of partitions and returns a map of partition number and initial offset
func fetchInitialOffsets(client *cluster.Client, topic, csg string, pars []int32) (map[int32]int64, error) {
	broker, err := client.Coordinator(csg)
	if err != nil {
		return nil, err
	}
	req := &sarama.OffsetFetchRequest{ConsumerGroup: csg}
	for _, par := range pars {
		req.AddPartition(topic, par)
	}
	out := make(map[int32]int64)
	resp, err := broker.FetchOffset(req)
	for _, par := range pars {
		block := resp.GetBlock(topic, par)
		if block == nil {
			return nil, sarama.ErrIncompleteResponse
		}

		if block.Err == sarama.ErrNoError {
			out[par] = block.Offset
		} else {
			return nil, block.Err
		}
	}
	return out, nil
}

func (me *MapReducer) receive(brokers []string, topic, csg string, outc chan<- Package) {
	client, consumer := connectKafka(brokers, topic, csg)
	endsignal := make(chan os.Signal, 1)
	signal.Notify(endsignal, os.Interrupt)

	sqmap := make(map[int32]*squasher.Squasher)
loop:
	for {
		select {
		case msg := <-consumer.Messages():
			if msg != nil {
				outc <- Package{key: string(msg.Key), data: msg.Value}
			}
			// exec.Add(string(msg.Key), Job{msg, me.term})
		case ntf := <-consumer.Notifications():
			if ntf == nil {
				break loop
			}

			pars := ntf.Current[topic]
			offsetM, err := fetchInitialOffsets(client, topic, csg, pars)
			if err != nil {
				log.Println("ERRFASDRT544", err)
				break loop
			}
			for k := range sqmap {
				delete(sqmap, k)
			}
			for _, par := range pars {
				sq := squasher.NewSquasher(offsetM[par], 10000) // 1M
				sqmap[par] = sq
				commitloop(consumer, topic, par, sq)
			}
		case err := <-consumer.Errors():
			if err != nil {
				log.Println("ERRDLOIDRFMS532 kafka error", err)
			}
		case <-endsignal:
			break loop
		}
	}
	if err := consumer.Close(); err != nil {
		log.Println("DDJDL45DL", err)
	}
}

func (me *MapReducer) Pipe(topic string) *MapReducer {
	me.Map(func(interface{}) string {
		return ""
	}).Reduce(func(key string, pkgs []Package) interface{} {
		return nil
	}).GlobalMap(nil, "", "", func(pkg interface{}) string { // auto save
		return ""
	}).Reduce(func(key string, pkgs []Package) interface{} { // if is the last one :save
		return nil
	})
	return me
}

// Push adds new job
func (me *MapReducer) Push(interface{}) {

}
