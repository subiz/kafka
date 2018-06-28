// +build ignore

package kafka

import (
	common "bitbucket.org/subiz/gocommon"
	"encoding/binary"
	"runtime"
	"testing"
	"time"
)

func skipTest(t *testing.T) {
	//t.Skip()
}

func TestPublishAndSubscribe(t *testing.T) {
	runtime.GOMAXPROCS(400)
	skipTest(t)
	n := 1000
	host := common.StartKafkaDev("")
	const cg = "con-4345081"
	const topic = "tmtopic43511031" //"test533124"
	datamap := make(map[uint64]bool)
	donechan := make(chan bool)

	// subscriber
	go func() {
		es := &EventStore{}
		es.Connect([]string{host}, []string{topic}, cg)
		return
		es.Listen(func(par int32, top string, data []byte, offset int64) bool {
			if topic != top {
				return true
			}
			key := binary.LittleEndian.Uint64(data)
			datamap[key] = true
			return len(datamap) < n
		}, func(m map[string][]int32) {
		})
		es.Close()
		donechan <- true
	}()

	// publisher
	es := &EventStore{}
	es.Connect([]string{host}, []string{}, "")
	for i := uint64(0); i < uint64(n)+1; i++ {
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(i))
		es.Publish(topic, data)
	}

	select {
	case <-donechan:
		return
	case <-time.After(time.Second * 30):
		t.Fatal("timeout")
	case <-EndSignal():
		t.Fatal("signal")
	}
}

func TestValidateTopicRule(t *testing.T) {
	skipTest(t)
	var testcase = []struct {
		in  string
		out bool
	}{
		{"ha$ivan", false},
		{"haivan", true},
		{"thanh._", true},
		{"thanh@!#", false},
		{"", false},
		{"asdkjfkjasdhfjkhaskjdhjkasdhfkhasjkdhkjhasdkjfhkasdjhfkjashdfkhaskjdfkjashdfkhasdjkfjkasdhfjkhaskdjfhkasjdhfjkhqweriusahdfiuqwhefihasiudhuiqwehifuhsauidfqwjkehfiuashdfjkhqwuiefhuiashdfuhwekjfchkjashedfkadskjfhkjasdhfjkhasdjkfhjkasldfjklashdfjklhasjdkfkjsadhfkjsahdfkjhsadjkfhkjsadhfjkhasdjkfhkasjdhfjkashdfjhasdjkfjsakdh", false},
	}
	for _, c := range testcase {
		out := validateTopicName(c.in)
		if out != c.out {
			t.Errorf("%s shoule be %v, got %v", c.in, c.out, out)
		}
	}
}

func TestAsyncPublish(t *testing.T) {
	n := 1000
	host := common.StartKafkaDev("")
	const cg = "con-4345082"
	const topic = "tmtopic43511032"
	datamap := make(map[uint64]bool)
	donechan := make(chan bool)

	// subscriber
	go func() {
		es := &EventStore{}
		es.Connect([]string{host}, []string{topic}, cg)
		return
		es.Listen(func(par int32, top string, data []byte, offset int64) bool {
			if topic != top {
				return true
			}
			key := binary.LittleEndian.Uint64(data)
			datamap[key] = true
			return len(datamap) < n
		}, func(m map[string][]int32) {
		})
		es.Close()
		donechan <- true
	}()

	// async publisher
	es := &EventStore{}
	es.Connect([]string{host}, []string{}, "")
	for i := uint64(0); i < uint64(n)+1; i++ {
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(i))
		es.AsyncPublish(topic, data)
	}

	select {
	case <-donechan:
		return
	case <-time.After(time.Second * 30):
		t.Fatal("timeout")
	case <-EndSignal():
		t.Fatal("signal")
	}
}
