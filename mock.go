package kafka

import (
	proto "github.com/golang/protobuf/proto"
	"bitbucket.org/subiz/gocommon"
)

type Event struct {
	Topic string
	Value string
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
		me.EventChan <- &Event{
			Topic: topic,
			Value: string(common.Protify(data[0].(proto.Message))),
			Partition: partition,
		}
	}()
}
