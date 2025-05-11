package kafka

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/subiz/header"
	"google.golang.org/grpc"
)

type MarkerServer struct {
	dead bool
	header.UnimplementedMarkerServer
	lock       *sync.Mutex
	offset     map[string]map[int64]bool
	grpcServer *grpc.Server
}

func NewMarkerServer() *MarkerServer {
	server := &MarkerServer{}
	server.lock = &sync.Mutex{}
	server.offset = map[string]map[int64]bool{}

	return server
}

func (me *MarkerServer) Shutdown() {
	me.dead = true
	if me.grpcServer != nil {
		me.grpcServer.GracefulStop()
	}
}

func (me *MarkerServer) Mark(ctx context.Context, req *header.MarkRequest) (*header.Empty, error) {
	key := fmt.Sprintf("%s.%d", req.GetTopic(), req.GetPartition())
	me.lock.Lock()
	defer me.lock.Unlock()

	alloffset := me.offset[key]
	if alloffset == nil {
		alloffset = map[int64]bool{}
		me.offset[key] = alloffset
	}
	for _, offset := range req.Offsets {
		alloffset[offset] = true
	}
	return &header.Empty{}, nil
}

func (me *MarkerServer) Commit(ctx context.Context, req *header.CommitRequest) (*header.Empty, error) {
	key := fmt.Sprintf("%s.%d", req.GetTopic(), req.GetPartition())
	me.lock.Lock()
	defer me.lock.Unlock()

	alloffset := me.offset[key]
	if alloffset == nil {
		alloffset = map[int64]bool{}
		me.offset[key] = alloffset
	}

	minoffset := req.Offset
	for offset, v := range alloffset {
		if v {
			if offset <= minoffset {
				alloffset[offset] = false
			}
		}
	}
	return &header.Empty{}, nil
}

func (me *MarkerServer) ListMarkers(ctx context.Context, req *header.MarkRequest) (*header.Markers, error) {
	key := fmt.Sprintf("%s.%d", req.GetTopic(), req.GetPartition())
	me.lock.Lock()
	defer me.lock.Unlock()

	alloffset := me.offset[key]
	if alloffset == nil {
		alloffset = map[int64]bool{}
		me.offset[key] = alloffset
	}

	offsets := []int64{}
	for offset, v := range alloffset {
		if v {
			offsets = append(offsets, offset)
		}
	}

	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	return &header.Markers{Offsets: offsets}, nil
}

func (me *MarkerServer) Serve() {
	ss := strings.Split("marker-0.marker:17695", ":")
	lis, err := net.Listen("tcp", ":"+ss[1])
	if err != nil {
		panic(err)
	}

	port, _ := strconv.Atoi(ss[1])
	grpcServer := header.NewShardServer2(port, 1)
	header.RegisterMarkerServer(grpcServer, me)
	me.grpcServer = grpcServer
	grpcServer.Serve(lis)
}
