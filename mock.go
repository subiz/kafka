package kafka

type Event struct {
	Topic string
	Value string
	Partition string
}

type EventStoreMock struct {
	MessageChan chan *Event
}

func (me *EventStoreMock) Config() {
	me.MessageChan = make(chan *Event)
}

func (me *EventStoreMock) Publish(topic string, data ...interface{}) {
	go func() {
		me.MessageChan <- &Event{
			Topic: topic,
			Value: data[0].(string),
			Partition: data[1].(string),
		}
	}()
}
