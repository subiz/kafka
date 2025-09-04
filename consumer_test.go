package kafka

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

var testbroker = "127.0.0.1:9092"

type TestJob struct {
	Id       string
	Key      string
	Duration int
}

type TestHandler struct {
	*sync.Mutex
	Start      int64
	HandleLogs map[string]int64 // jobid -> executed
	DupCount   int
	TotalCalls int
	TotalJobs  int
}

func NewTestHandler() (func(string, any), func() *TestHandler) {
	deltaT := 10
	th := &TestHandler{Mutex: &sync.Mutex{}, HandleLogs: map[string]int64{}}
	return func(key string, payload any) {
			job := payload.(*TestJob)
			th.Lock()
			if th.Start == 0 {
				th.Start = time.Now().UnixMilli()
			}
			if th.HandleLogs[job.Id] != 0 {
				th.DupCount++
			} else {
				th.HandleLogs[job.Id] = time.Now().UnixMilli()
			}
			th.TotalCalls++
			th.TotalJobs = len(th.HandleLogs)
			th.Unlock()
			time.Sleep(time.Duration(job.Duration*deltaT) * time.Millisecond)
		}, func() *TestHandler {
			return th
		}
}

type JobGenerator struct {
	TotalJobs int
	TotalKeys int
}

func NewJobGenerator(idprefix string, totalJobs, totalKeys int) chan *TestJob {
	if totalKeys < 1 {
		totalKeys = 1
	}
	ch := make(chan *TestJob, 10)

	keys := make([]string, totalKeys)
	for i := range totalKeys {
		keys[i] = fmt.Sprintf("key-%d-%d-%d", time.Now().UnixNano(), rand.Intn(int(time.Now().UnixMilli())), i)
	}

	if totalJobs <= 0 {
		go close(ch)
		return ch
	}

	go func() {
		for i := range totalJobs {
			time.Sleep(2 * time.Millisecond)
			num := rand.Intn(totalKeys)
			key := keys[num]
			ch <- &TestJob{
				Key:      key,
				Id:       fmt.Sprintf("%s-%08d", idprefix, i),
				Duration: int(rand.Intn(10)),
			}
		}
	}()
	return ch
}

func SetupConsumerTest() string {
	topic := fmt.Sprintf("testtopic-%d", time.Now().UnixNano()) // see ./setuptest.sh
	output, err := exec.Command("/bin/bash", "./setuptest.sh", topic).CombinedOutput()
	if err != nil {
		fmt.Println("OUT", string(output))
		panic(err)
	}
	return topic
}

func makeSureConsumedAllMessages(t *testing.T, topic, consumergroup string) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	//  config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// 1. find number of partitions
	pclient, err := sarama.NewClient([]string{testbroker}, config)
	if err != nil {
		panic(err)
	}
	latestOffsetM := map[int32]int64{}
	partitions, err := pclient.Partitions(topic)
	for _, partition := range partitions {
		latestOffset, err := pclient.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		latestOffsetM[partition] = latestOffset
	}

	if err != nil {
		panic(err)
	}
	defer pclient.Close()

	admin, err := sarama.NewClusterAdminFromClient(pclient)
	if err != nil {
		panic(err)
	}
	defer admin.Close() // Ensure the admin client is closed

	topicPartitions := map[string][]int32{
		topic: partitions,
	}
	groupOffsets, err := admin.ListConsumerGroupOffsets(consumergroup, topicPartitions)
	if err != nil {
		panic(err)
	}

	consumerOffsetM := map[int32]int64{}
	for _, partition := range partitions {
		block := groupOffsets.GetBlock(topic, partition)
		consumerOffsetM[partition] = block.Offset
	}
	time.Sleep(20 * time.Second)
	for _, partition := range partitions {
		if consumerOffsetM[partition] != latestOffsetM[partition] {
			t.Errorf("MISSING OFFSET OF PARTITION %d. Expect %d, got %d", partition, latestOffsetM[partition], consumerOffsetM[partition])
		}
	}
}

func TestConsumerNormal(t *testing.T) {
	topic := SetupConsumerTest()
	consumergroup := "consumer1"

	var totalRandomJobs = 10000
	var totalHotJobs = 2000

	go func() {
		jobchan := NewJobGenerator("normal", totalRandomJobs, totalRandomJobs/10)
		for job := range jobchan {
			b, _ := json.Marshal(job)
			Publish(testbroker, topic, b, job.Key)
		}
	}()

	// hot key
	go func() {
		jobchan := NewJobGenerator("hot", totalHotJobs, totalHotJobs/500)
		for job := range jobchan {
			b, _ := json.Marshal(job)
			Publish(testbroker, topic, b, job.Key)
		}
	}()

	handlerf, statf := NewTestHandler()
	var consumer *Consumer
	go func() {
		var err error
		consumer, err = Consume(testbroker, "consumer1", topic, func(partition int32, offset int64, data []byte, key string) {
			job := &TestJob{}
			json.Unmarshal(data, job)
			handlerf(key, job)
		})
		if err != nil {
			panic(err)
		}
	}()

	for {
		time.Sleep(5000 * time.Millisecond)
		status := statf()
		fmt.Println("PROGRESS", "DUP", status.DupCount, "TOTAL CALLS", status.TotalCalls, "TOTALJOB", status.TotalJobs)
		if status.TotalJobs == totalRandomJobs+totalHotJobs {
			time.Sleep(20 * time.Second) // wait for commit
			break                        // done
		}
	}

	consumer.CloseAsync()
	makeSureConsumedAllMessages(t, topic, consumergroup)
}

func TestConsumerCrashingConsumer(t *testing.T) {
	done := false
	topic := SetupConsumerTest()
	consumergroup := "consumer1"
	var totalRandomJobs = 10_000
	var totalHotJobs = 200
	lock := &sync.Mutex{}
	publishcount := 0
	go func() {
		jobchan := NewJobGenerator("normal", totalRandomJobs, totalRandomJobs/10)
		for msg := range jobchan {
			b, _ := json.Marshal(msg)
			Publish(testbroker, topic, b, msg.Key)
			lock.Lock()
			publishcount++
			lock.Unlock()
		}
	}()

	// hot key
	go func() {
		jobchan := NewJobGenerator("hot", totalHotJobs, totalHotJobs/500)
		for msg := range jobchan {
			b, _ := json.Marshal(msg)
			Publish(testbroker, topic, b, msg.Key)
			lock.Lock()
			publishcount++
			lock.Unlock()
		}
	}()

	handlerf, statf := NewTestHandler()
	recv := 0

	var consumer *Consumer
	go func() {
		for !done {
			time.Sleep(22 * time.Second)
			consumer.CloseAsync()
		}
	}()

	go func() {
		for !done {
			var err error
			consumer, err = Consume(testbroker, consumergroup, topic, func(partition int32, offset int64, data []byte, key string) {
				lock.Lock()
				recv++
				lock.Unlock()
				job := &TestJob{}
				json.Unmarshal(data, job)
				handlerf(key, job)
			})
			if err != nil {
				panic(err)
			}
			fmt.Println("CRASHED", "RESTART IN 10sec...")
			time.Sleep(10 * time.Second)
		}
	}()

	for {
		time.Sleep(5000 * time.Millisecond)
		status := statf()
		fmt.Println("PROGRESS", "DUP", status.DupCount, "TOTAL CALLS", status.TotalCalls, "TOTALJOB", status.TotalJobs, "PUBLISHED", publishcount, recv)
		if status.TotalJobs == totalRandomJobs+totalHotJobs {
			time.Sleep(20 * time.Second) // wait for commit
			break                        // done
		}
	}
	consumer.CloseAsync()
	done = true
	makeSureConsumedAllMessages(t, topic, consumergroup)
}
