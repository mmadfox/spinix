package cluster

import (
	"sync"

	h3geodist "github.com/mmadfox/go-h3geo-dist"
)

type Router struct {
	mu sync.RWMutex
	hd *h3geodist.Distributed
	ml *Memberlist
	nl *nodeList
}

func NewRouter(
	hd *h3geodist.Distributed,
	ml *Memberlist,
) *Router {
	router := Router{
		hd: hd,
		ml: ml,
		nl: newNodeList(),
	}

	ml.OnJoinFunc(router.handleNodeJoin)
	ml.OnLeaveFunc(router.handleNodeLeave)
	ml.OnUpdateFunc(router.handleNodeUpdate)
	ml.OnChangeFunc(router.handleChangeState)

	return &router
}

func (r *Router) handleNodeJoin(n *Node) {
	if err := r.hd.Add(n.Host()); err != nil {
		return
	}
	r.nl.add(n)
}

func (r *Router) handleNodeLeave(n *Node) {
	r.hd.Remove(n.Host())
	r.nl.remove(n)
}

func (r *Router) handleNodeUpdate(n *Node) {
	r.nl.removeByHost(n.Host())
	r.hd.Remove(n.Host())
	if err := r.hd.Add(n.Host()); err != nil {
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
