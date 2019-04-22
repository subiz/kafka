package kafka

import (
	"github.com/golang/protobuf/proto"
	cpb "github.com/subiz/header/common"
	"log"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var g_brokers = []string{"dev.subiz.net:9092"}

func TestRebalance(t *testing.T) {
	now := time.Now().String()
	go func() {
		pub := NewPublisher(g_brokers)
		for i := 0; i < 1000; i++ {
			is := strconv.Itoa(i)
			pub.Publish("test10",
				&cpb.Id{Ctx: &cpb.Context{SubTopic: "x"}, AccountId: is}, -1, is+now)
		}
	}()
	var ids = &sync.Map{}
	var done = make(chan bool)
	h := NewHandler(g_brokers, "grp1452", "test10")
	go h.Serve(map[string]func(*cpb.Context, []byte){
		"": func(ctx *cpb.Context, b []byte) {
			select {
			case done <- true:
			default:
			}
			time.Sleep(10 * time.Hour)
		},
	}, func(pars []int32) { log.Println("#5829222 got partition", pars) })
	<-done
	println("DONE")
	h.Stop()

	for i := 0; i < 3; i++ {
		go h.Serve(map[string]func(*cpb.Context, []byte){
			"": func(ctx *cpb.Context, b []byte) {
				if !strings.Contains(ctx.GetKafkaKey(), now) {
					return
				}
				id := &cpb.Id{}
				proto.Unmarshal(b, id)
				ids.Store(id.GetAccountId(), true)
				count := 0
				ids.Range(func(_, _ interface{}) bool { count++; return true })
				if count == 1000 {
					done <- true
				}
			},
		}, func(pars []int32) {
			log.Println("#5829222 got partition", pars)
		})
	}
	<-done
	time.Sleep(2 * time.Second)
	h.Stop()
}

// ./kafka-topics.sh --zookeeper zookeeper --alter --topic test10 --partitions 10
func TestSimple(t *testing.T) {
	now := time.Now().String()
	go func() {
		pub := NewPublisher(g_brokers)
		for i := 0; i < 1000; i++ {
			is := strconv.Itoa(i)
			pub.Publish("test10",
				&cpb.Id{Ctx: &cpb.Context{SubTopic: "x"}, AccountId: is}, -1, is+now)
		}
	}()
	var ids = &sync.Map{}
	var done = make(chan bool)
	h := NewHandler(g_brokers, "grp1", "test10")
	go h.Serve(map[string]func(*cpb.Context, []byte){
		"": func(ctx *cpb.Context, b []byte) {
			if !strings.Contains(ctx.GetKafkaKey(), now) {
				return
			}
			id := &cpb.Id{}
			proto.Unmarshal(b, id)
			ids.Store(id.GetAccountId(), true)
			count := 0
			ids.Range(func(_, _ interface{}) bool { count++; return true })
			if count == 1000 {
				done <- true
			}
		},
	}, func(pars []int32) { log.Println("#5829222 got partition", pars) })
	<-done
	time.Sleep(2 * time.Second)
}
