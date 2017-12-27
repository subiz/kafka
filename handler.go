package kafka

import (
	"github.com/golang/protobuf/proto"
	"context"
	"reflect"
	"fmt"
	"bitbucket.org/subiz/gocommon"
	compb "bitbucket.org/subiz/header/common"
)

type ft struct {
	t reflect.Type
	f reflect.Value
}
type Router struct {
	routes map[string]ft
	rr common.RequestReply
	es *EventStore
	service string
	serviceid string
}

type R map[fmt.Stringer]interface{}

func NewRouter(es *EventStore, service, serviceid string, routes R) *Router {
	rs := make(map[string]ft)
	for k, v := range routes {
		f := reflect.ValueOf(v)
		ptype := f.Type().In(0)
		rs[k.String()] = ft{t: ptype, f: f}
	}
	return &Router{
		service: service,
		serviceid: serviceid,
		routes: rs,
	}
}

type stringer struct {
	s string
}

func (s stringer) String() string {
	return s.s
}

func Str(s string) fmt.Stringer {
	return &stringer{s:s}
}

func (r *Router) Handle(octx *context.Context, val []byte, ret proto.Message, err error) {
	payload := &compb.Empty{}
	var pctx *compb.Context
	pctx, *octx = r.rr.ParseContext(val, payload) // parse proto 1 time to get topic
	defer func() {
		r.rr.HandleReply(recover(), r.es, pctx, err, ret, r.service, r.serviceid)
	}()

	handler := r.routes[pctx.GetTopic()]
	if handler.t == nil {
		common.LogErr("unknown route")
		return
	}
	pptr := reflect.New(handler.t.Elem())
	pptr.Elem().Set(reflect.Zero(handler.t.Elem()))
	r.rr.ParseContext(val, pptr.Interface().(common.Payload))
	handler.f.Call([]reflect.Value{pptr})
}
