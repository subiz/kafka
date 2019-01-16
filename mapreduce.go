package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"github.com/subiz/squasher"
	"log"
	"strings"
	"time"
)

const MAP = "map"
const REDUCE = "reduce"
const BEGIN = "begin"
const GLOBALMAP = "global_map"

type KafkaMessage struct {
	key       string
	partition int32
	offset    int64
	data      interface{}
}

type Package struct {
	key  string
	data interface{}

	donec        chan KafkaMessage
	baseMessages []KafkaMessage
}

type Phase struct {
	funcType string
	inchan   <-chan Package
	outchan  chan<- Package
}

type MapReducer struct {
	csm     *cluster.Consumer
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
	return me
}

func (me *MapReducer) GlobalMap(brokers []string, topic, group string, f func(interface{}) string) Mapper {
	// take the last in
	// TODO: check err
	lastphase := me.phases[len(me.phases)-1]

	publisher := newHashedAsyncPublisher(brokers)
	inchan := publisher.Input()

	outc := make(chan Package)

	go func() {
		for msg := range publisher.Successes() {
			pkg := msg.Metadata.(Package)
			for _, msg := range pkg.baseMessages {
				pkg.donec <- msg
			}
		}
	}()

	go func() {
		for err := range publisher.Errors() {
			fmt.Println(err)
		}
	}()

	for pkg := range lastphase.inchan {
		key := f(pkg.data)
		if msg := packMsg(topic, pkg, key); msg != nil {
			inchan <- msg
		}
	}

	go func() {
		donec := make(chan KafkaMessage)
		c := receive(brokers, topic, group, donec)
		for msg := range c {
			outc <- Package{
				key:   string(msg.key),
				data:  msg.data,
				donec: donec,
				baseMessages: []KafkaMessage{{
					partition: msg.partition,
					offset:    msg.offset,
				}},
			}
		}
	}()
	me.phases = append(me.phases, Phase{funcType: GLOBALMAP, outchan: outc})

	return me
}

func packMsg(topic string, pkg Package, key string) *sarama.ProducerMessage {
	var value []byte
	var err error
	data := pkg.data
	switch data := data.(type) {
	case proto.Message:
		value, err = proto.Marshal(data)
		if err != nil {
			log.Printf("kafka error, unable to marshal: %v\n", data)
			return nil
		}
	case []byte:
		value = data
	case string:
		value = []byte(data)
	default:
		log.Println("value should be type of proto.Message, []byte or string")
		return nil
	}

	return &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(value),
		Metadata:  pkg,
	}
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
		pkg.key = f(pkg.data)
		outc <- pkg
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
			basemsgs := make([]KafkaMessage, 0)
			var donec chan KafkaMessage
			for _, pkg := range pkgs {
				donec = pkg.donec
				basemsgs = append(basemsgs, pkg.baseMessages...)
			}
			data := f(key, pkgs)
			outc <- Package{key: key, data: data, donec: donec, baseMessages: basemsgs}
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
func fetchInitialOffsets(client *cluster.Client, topic, group string, pars []int32) (map[int32]int64, error) {
	broker, err := client.Coordinator(group)
	if err != nil {
		return nil, err
	}
	req := &sarama.OffsetFetchRequest{ConsumerGroup: group}
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

func receive(brokers []string, topic, group string, donec <-chan KafkaMessage) <-chan KafkaMessage {
	client, consumer := connectKafka(brokers, topic, group)
	outc := make(chan KafkaMessage)
	sqmap := make(map[int32]*squasher.Squasher)
	go func() {
		defer consumer.Close()
		for {
			select {
			case msg, more := <-donec:
				if more {
					if sq, ok := sqmap[msg.partition]; ok {
						sq.Mark(msg.offset)
					}
				}
			case msg := <-consumer.Messages():
				if msg != nil {
					outc <- KafkaMessage{
						key:       string(msg.Key),
						offset:    msg.Offset,
						partition: msg.Partition,
						data:      msg.Value,
					}
				}
			case ntf := <-consumer.Notifications():
				if ntf == nil {
					return
				}
				pars := ntf.Current[topic]
				offsetM, err := fetchInitialOffsets(client, topic, group, pars)
				if err != nil {
					log.Println("ERRFASDRT544", err)
					return
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
			}
		}
	}()
	return outc
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
