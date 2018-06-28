package kafka

import (
	"github.com/golang/protobuf/proto"
)

type Event struct {
	Topic     string
	Value     string
	Partition string
}

type EventStoreMock struct {
	EventChan chan *Event
}

func (me *EventStoreMock) Config() {
	me.EventChan = make(chan *Event)
}

func (me *EventStoreMock) Publish(topic string, data ...interface{}) {
	go func() {
		partition := ""
		if len(data) > 2 {
			partition, _ = data[1].(string)
		}
		bytes, err := proto.Marshal(data[0].(proto.Message))
		if err != nil {
			panic(err)
		}
		me.EventChan <- &Event{Topic: topic, Value: string(bytes), Partition: partition}
	}()
}
