package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/subiz/errors"
	"github.com/subiz/executor"
	"log"
	"os"
	"os/signal"
	"time"
)

type Consumers struct {
	consumers []sarama.PartitionConsumer

	// hs map of topic -> func
	hs map[string]handlerFunc

	// exec is job balancer
	exec *executor.Executor
}

func NewUnoffsetConsumer(brokers []string, topic string, maxworkers, maxlag uint) (*Consumers, error) {
	c := sarama.NewConfig()
	c.Consumer.MaxWaitTime = 10000 * time.Millisecond
	//c.Consumer.Offsets.Retention = 0
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetNewest

	// get partitions for topics
	consumer, err := sarama.NewConsumer(brokers, c)
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_kafka_error)
	}
	pars, err := consumer.Partitions(topic)
	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_kafka_error)
	}

	cs := make([]sarama.PartitionConsumer, 0)
	for _, par := range pars {
		parCsm, err := consumer.ConsumePartition(topic, par, sarama.OffsetNewest)
		if err != nil {
			return nil, errors.Wrap(err, 500, errors.E_kafka_error)
		}
		cs = append(cs, parCsm)
	}
	consumers := &Consumers{}
	consumers.consumers = cs
	consumers.exec = executor.New(maxworkers, maxlag, consumers.handleJob)
	return consumers, nil
}

func (cs *Consumers) handleJob(_ string, job interface{}) {
	mes := job.(*sarama.ConsumerMessage)
	err := callHandler(cs.hs, mes.Value, 0, mes.Partition, mes.Offset, string(mes.Key))
	if err != nil && err != notfounderr {
		log.Printf("topic %s:%d[%d]\n", mes.Topic, mes.Partition, mes.Offset)
		log.Println(err)
	}
}

func (cs *Consumers) Serve(handler R, nh func([]int32)) error {
	cs.hs = convertToHandleFunc(handler)
	errchan := make(chan error)
	for _, consumer := range cs.consumers {
		go func() {
			endsignal := EndSignal()
		loop:
			for {
				select {
				case msg, more := <-consumer.Messages():
					if !more || msg == nil {
						break loop
					}
					cs.exec.Add(string(msg.Key), msg)
				case err := <-consumer.Errors():
					if err != nil {
						log.Println("kafka error", err)
					}
				case <-endsignal:
					break loop
				}
			}
			errchan <- consumer.Close()
		}()
	}
	return <-errchan
}

func EndSignal() chan os.Signal {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	return signals
}
