package cluster

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/uber/h3-go/v3"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
	"go.uber.org/zap"

	h3geodist "github.com/mmadfox/go-h3geo-dist"
)

type router struct {
	mu       sync.RWMutex
	hd       *h3geodist.Distributed
	nl       *nodeInfoList
	numNodes int32
	routes   map[uint64]*pb.Route
	logger   *zap.Logger
}

func newRouter(hd *h3geodist.Distributed, logger *zap.Logger) *router {
	router := router{
		hd:     hd,
		nl:     newNodeList(),
		logger: logger,
	}
	return &router
}

func (r *router) Lookup(index h3.H3Index) (h3geodist.Cell, bool) {
	return r.hd.Lookup(index)
}

func (r *router) String() string {
	return r.nl.String()
}

func (r *router) NumVNodes() int {
	return int(r.hd.VNodes())
}

func (r *router) SetNumNodes(val int) {
	atomic.StoreInt32(&r.numNodes, int32(val))
}

func (r *router) NumNodes() int32 {
	return atomic.LoadInt32(&r.numNodes)
}

func (r *router) AddNode(n nodeInfo) error {
	if err := r.hd.Add(n.Addr()); err != nil {
		return err
	}
	r.nl.add(n)
	return nil
}

func (r *router) RemoveNode(n nodeInfo) {
	r.hd.Remove(n.Addr())
	r.nl.remove(n)
}

func (r *router) UpdateNode(n nodeInfo) error {
	r.nl.removeByAddr(n.Addr())
	r.hd.Remove(n.Addr())
	if err := r.hd.Add(n.Addr()); err != nil {
		return err
	}
	r.nl.add(n)
	return nil
}

func (r *router) EachVNodeInfo(fn func(id uint64, addr string) bool) {
	r.hd.EachVNode(fn)
}

func (r *router) EachNodeInfo(fn func(ni nodeInfo)) {
	r.nl.mu.RLock()
	defer r.nl.mu.RUnlock()
	for _, ni := range r.nl.store {
		fn(ni)
	}
}

func (r *router) Routes() []*pb.Route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	routes := make([]*pb.Route, 0, len(r.routes))
	for _, route := range r.routes {
		routes = append(routes, route)
	}
	return routes
}

func (r *router) SetRoutes(routes []*pb.Route) {
	rm := make(map[uint64]*pb.Route)
	for i := 0; i < len(routes); i++ {
		route := routes[i]
		rm[route.VnodeId] = route
	}
	r.mu.Lock()
	r.routes = rm
	r.mu.Unlock()
}

type nodeInfoList struct {
	mu    sync.RWMutex
	store map[uint64]nodeInfo
}

func newNodeList() *nodeInfoList {
	return &nodeInfoList{store: make(map[uint64]nodeInfo)}
}

func (nl *nodeInfoList) String() string {
	nl.mu.RLock()
	defer nl.mu.RUnlock()
	nodes := make([]string, 0, len(nl.store))
	for _, ni := range nl.store {
		nodes = append(nodes, ni.Addr())
	}
	return strings.Join(nodes, ",")
}

func (nl *nodeInfoList) add(n nodeInfo) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.store[n.ID()] = n
}

func (nl *nodeInfoList) remove(n nodeInfo) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	delete(nl.store, n.ID())
}

func (nl *nodeInfoList) removeByAddr(addr string) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	for id, node := range nl.store {
		if node.Addr() == addr {
			delete(nl.store, id)
		}
	}
}
