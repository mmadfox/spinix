package cluster

import (
	"strings"
	"sync"
	"sync/atomic"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
	"go.uber.org/zap"

	h3geodist "github.com/mmadfox/go-h3geo-dist"
)

type router struct {
	mu         sync.RWMutex
	hd         *h3geodist.Distributed
	nl         *nodeInfoList
	cli        *pool
	numNodes   int32
	routes     map[uint64]*pb.Route
	pVNodeList *vnodeList
	sVNodeList *vnodeList
	logger     *zap.Logger
}

func newRouter(
	hd *h3geodist.Distributed,
	cli *pool,
	logger *zap.Logger,
	pVNodeList *vnodeList,
	sVNodeList *vnodeList,
) *router {
	router := router{
		hd:         hd,
		nl:         newNodeList(),
		cli:        cli,
		logger:     logger,
		pVNodeList: pVNodeList,
		sVNodeList: sVNodeList,
	}
	return &router
}

func (r *router) String() string {
	return r.nl.String()
}

func (r *router) UpdateNumNodes(val int) {
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
