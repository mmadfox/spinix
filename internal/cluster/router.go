package cluster

import (
	"sync"

	h3geodist "github.com/mmadfox/go-h3geo-dist"
)

type Router struct {
	mu  sync.RWMutex
	hd  *h3geodist.Distributed
	ml  *nodeman
	nl  *nodeInfoList
	cli *Client
}

func NewRouter(
	hd *h3geodist.Distributed,
	ml *nodeman,
	cli *Client,
) *Router {
	router := Router{
		hd:  hd,
		ml:  ml,
		nl:  newNodeList(),
		cli: cli,
	}

	ml.OnJoinFunc(router.handleNodeJoin)
	ml.OnLeaveFunc(router.handleNodeLeave)
	ml.OnUpdateFunc(router.handleNodeUpdate)
	ml.OnChangeFunc(router.handleChangeState)

	return &router
}

func (r *Router) handleNodeJoin(n *nodeInfo) {
	if err := r.hd.Add(n.Addr()); err != nil {
		return
	}
	r.nl.add(n)
}

func (r *Router) handleNodeLeave(n *nodeInfo) {
	r.hd.Remove(n.Addr())
	r.nl.remove(n)
}

func (r *Router) handleNodeUpdate(n *nodeInfo) {
	r.nl.removeByAddr(n.Addr())
	r.hd.Remove(n.Addr())
	if err := r.hd.Add(n.Addr()); err != nil {
		return
	}
	r.nl.add(n)
}

func (r *Router) handleChangeState() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.ml.IsCoordinator() {
		return
	}

	// TODO:
}
