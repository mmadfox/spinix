package cluster

import (
	"sync"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
	"go.uber.org/zap"

	h3geodist "github.com/mmadfox/go-h3geo-dist"
)

type router struct {
	mu         sync.RWMutex
	hd         *h3geodist.Distributed
	nl         *nodeInfoList
	cli        *pool
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

func (r *router) AddNode(n *nodeInfo) error {
	if err := r.hd.Add(n.Addr()); err != nil {
		return err
	}
	r.nl.add(n)
	return nil
}

func (r *router) RemoveNode(n *nodeInfo) {
	r.hd.Remove(n.Addr())
	r.nl.remove(n)
}

func (r *router) UpdateNode(n *nodeInfo) error {
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
	store map[uint64]*nodeInfo
}

func newNodeList() *nodeInfoList {
	return &nodeInfoList{store: make(map[uint64]*nodeInfo)}
}

func (nl *nodeInfoList) add(n *nodeInfo) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.store[n.ID()] = n
}

func (nl *nodeInfoList) remove(n *nodeInfo) {
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

//func (r *router) ChangeState() {
//	routes := make(map[uint64]*pb.Route)
//	r.hd.EachVNode(func(vnode uint64, addr string) bool {
//		route := &pb.Route{
//			Vnode:     vnode,
//			Primary:   r.makePrimaryList(addr, vnode),
//			Secondary: r.makeSecondaryList(addr, vnode),
//		}
//		routes[vnode] = route
//		return true
//	})
//	r.mu.Lock()
//	r.routes = routes
//	r.mu.Unlock()
//}

//func (r *router) makePrimaryList(addr string, id uint64) []*pb.NodeInfo {
//	//ctx := context.Background()
//	//client, err := r.cli.NewClient(ctx, addr)
//	//if err != nil {
//	//	log.Println(err)
//	//	return []*pb.NodeInfo{}
//	//}
//	//log.Println("CLI", client)
//	//resp, err := client.VNodeStats(ctx, &pb.VNodeStatsRequest{})
//	//log.Println(addr, resp, err)
//	vn := r.pVNodeList.ByID(id)
//	owners := make([]nodeInfo, len(vn.owners))
//	copy(owners, vn.owners)
//	nodeInfo := nodeInfoFromAddr(addr)
//	if len(owners) == 0 {
//		return []*pb.NodeInfo{nodeInfo.ToProto()}
//	}
//	log.Println("owners", len(owners))
//	return nil
//}
//
//func (r *router) makeSecondaryList(addr string, id uint64) []*pb.NodeInfo {
//	// TODO:
//	return nil
//}
