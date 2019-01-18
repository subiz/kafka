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

type Message interface {
	GetKey() string
	GetData() interface{}
	Commit() // mark as done
}

//
type MRMessage struct {
	key          string
	data         interface{}
	baseMessages []Message
}

func (me MRMessage) GetKey() string       { return me.key }
func (me MRMessage) GetData() interface{} { return me.data }
func (me MRMessage) Commit() {
	for _, base := range me.baseMessages {
		base.Commit()
	}
}

type KafkaMessage struct {
	key       string
	partition int32
	offset    int64
	data      interface{}
	donec     chan<- KafkaMessage
}

func (me KafkaMessage) GetKey() string       { return me.key }
func (me KafkaMessage) GetData() interface{} { return me.data }
func (me KafkaMessage) Commit()              { me.donec <- me }

type Phase struct {
	funcType string
	inchan   <-chan Message
	outchan  chan<- Message
}

type MapReducer struct {
	phases []Phase
}

type MapF func(interface{}) string // receive an object and return it's key
type ReduceF func(string, interface{}, interface{}) interface{}
type TransformF func(interface{}) interface{}
type Sinker func(<-chan Message) chan<- Message

type Reducer interface {
	Map(f MapF) Mapper
	GlobalMap(brokers []string, topic, csm string, f MapF) Mapper
	Transform(TransformF) Reducer
	Sink(topic string, f MapF)
}

type Mapper interface {
	Reduce(f ReduceF) Reducer
}

func NewMapReducer(csg, topic string) *MapReducer {
	me := &MapReducer{}
	return me
}

func (me *MapReducer) CreateKafkaSinker(brokers []string, topic string, f MapF) Sinker {
	publisher := newHashedAsyncPublisher(brokers)
	publishChan := publisher.Input()

	go func() {
		for {
			select {
			case msg := <-publisher.Successes():
				msg.Metadata.(Message).Commit()
			case err := <-publisher.Errors():
				fmt.Println(err)
			}
		}
	}()

	return func(inchan <-chan Message) chan<- Message {
		for mrmsg := range inchan {
			key := f(mrmsg.GetData())
			if msg := packMsg(topic, mrmsg, key); msg != nil {
				publishChan <- msg
			}
		}
	}
}

func (me *MapReducer) Sink(brokers []string, topic string, f MapF) {
	lastphase := me.phases[len(me.phases)-1]

	publisher := newHashedAsyncPublisher(brokers)
	inchan := publisher.Input() // used to publish to kafka

	go func() {
		for {
			select {
			case msg := <-publisher.Successes():
				msg.Metadata.(Message).Commit()
			case err := <-publisher.Errors():
				fmt.Println(err)
			}
		}
	}()

	for mrmsg := range lastphase.inchan {
		key := f(mrmsg.GetData())
		if msg := packMsg(topic, mrmsg, key); msg != nil {
			inchan <- msg // publish to kafka
		}
	}
	return me
}

func (me *MapReducer) GlobalMap(brokers []string, topic, group string, f MapF) Mapper {
	// take the last in
	// TODO: check err
	lastphase := me.phases[len(me.phases)-1]

	publisher := newHashedAsyncPublisher(brokers)
	inchan := publisher.Input() // used to publish to kafka

	go func() {
		for {
			select {
			case msg := <-publisher.Successes():
				msg.Metadata.(Message).Commit()
			case err := <-publisher.Errors():
				fmt.Println(err)
			}
		}
	}()

	for mrmsg := range lastphase.inchan {
		key := f(mrmsg.GetData())
		if msg := packMsg(topic, mrmsg, key); msg != nil {
			inchan <- msg // publish to kafka
		}
	}

	outc := make(chan Message)
	go func() {
		donec := make(chan KafkaMessage)
		for msg := range receive(brokers, topic, group, donec) {
			outc <- msg
		}
	}()
	me.phases = append(me.phases, Phase{funcType: GLOBALMAP, outchan: outc})
	return me
}

func packMsg(topic string, msg Message, key string) *sarama.ProducerMessage {
	var value []byte
	var err error
	data := msg.GetData()
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
		Metadata:  msg,
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
func (me *MapReducer) Map(f MapF) Mapper {
	lastphase := me.phases[len(me.phases)-1]
	outc := make(chan Message)

	for msg := range lastphase.inchan {
		key := f(msg.GetData())
		outc <- MRMessage{
			key:          key,
			data:         msg.GetData(),
			baseMessages: []Message{msg},
		}
	}
	me.phases = append(me.phases, Phase{funcType: GLOBALMAP, outchan: outc})
	return me
}

func (me *MapReducer) Transform(f TransformF) Reducer {
	lastphase := me.phases[len(me.phases)-1]
	outc := make(chan Message)

	for msg := range lastphase.inchan {
		newdata := f(msg.GetData())
		outc <- MRMessage{
			data:         newdata,
			baseMessages: []Message{msg},
		}
	}
	me.phases = append(me.phases, Phase{funcType: GLOBALMAP, outchan: outc})
	return me
}

func (me *MapReducer) Run() {

}

// Reduce registries a map function
func (me *MapReducer) Reduce(f ReduceF) Reducer {
	outc := make(chan Message)

	reduceM := make(map[string]MRMessage) // key -> MRMessage
	bulkcount := 0
	clear := func() {
		for key, mrmsg := range reduceM {
			outc <- mrmsg
			delete(reduceM, key)
		}
		bulkcount = 0
	}

	go func() {
		// take the last in
		lastphase := me.phases[len(me.phases)-1]
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case inmsg := <-lastphase.inchan:
				key := inmsg.GetKey()
				data := inmsg.GetData()
				basemsgs := make([]Message, 0)
				mrmsg, found := reduceM[key]
				if found {
					data = f(key, mrmsg.GetData(), data)
					basemsgs = append(basemsgs, mrmsg.baseMessages...)
				}
				basemsgs = append(basemsgs, inmsg)
				reduceM[key] = MRMessage{key: key, data: data, baseMessages: basemsgs}

				bulkcount++
				if bulkcount > 1000 {
					clear()
				}
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

func Source() *MapReducer {
	outc := make(chan Message)
	go func() {
		donec := make(chan KafkaMessage)
		for msg := range receive(brokers, topic, group, donec) {
			outc <- msg
		}
	}()
	me.phases = append(me.phases, Phase{funcType: GLOBALMAP, outchan: outc})
	return me
}

func (me *MapReducer) Pipe(topic string) *MapReducer {
	Source().Map(func(interface{}) string {
		return ""
	}).Reduce(func(key string, a, b interface{}) interface{} {
		return nil
	}).GlobalMap(nil, "", "", func(pkg interface{}) string { // auto save
		return ""
	}).Reduce(func(key string, a, b interface{}) interface{} { // if is the last one :save
		return nil
	}).Sink(func(string, interface{}) {
	})
	return me
}

// Push adds new job
func (me *MapReducer) Push(interface{}) {

}
