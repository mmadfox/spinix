package cluster

import (
	"sync"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"

	h3geodist "github.com/mmadfox/go-h3geo-dist"
)

type router struct {
	mu         sync.RWMutex
	hd         *h3geodist.Distributed
	nl         *nodeInfoList
	cli        *client
	routes     map[uint64]*pb.Route
	pVNodeList *vnodeList
	sVNodeList *vnodeList
}

func newRouter(
	hd *h3geodist.Distributed,
	cli *client,
) *router {
	router := router{
		hd:  hd,
		nl:  newNodeList(),
		cli: cli,
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

func (r *router) ChangeState() {
	routes := make(map[uint64]*pb.Route)
	r.hd.EachVNode(func(vnode uint64, addr string) bool {
		route := &pb.Route{
			Vnode:     vnode,
			Primary:   r.makePrimaryList(addr, vnode),
			Secondary: r.makeSecondaryList(addr, vnode),
		}
		routes[vnode] = route
		return true
	})
	r.mu.Lock()
	r.routes = routes
	r.mu.Unlock()
}

func (r *router) makePrimaryList(addr string, id uint64) []*pb.NodeInfo {
	vn := r.pVNodeList.ByID(id)
	owners := make([]nodeInfo, len(vn.owners))
	copy(owners, vn.owners)
	nodeInfo := nodeInfoFromAddr(addr)
	if len(owners) == 0 {
		return []*pb.NodeInfo{nodeInfo.ToProto()}
	}
	return nil
}

func (r *router) makeSecondaryList(addr string, id uint64) []*pb.NodeInfo {
	// TODO:
	return nil
}
