package cluster

import (
	"sync"

	h3geodist "github.com/mmadfox/go-h3geo-dist"
)

type Router struct {
	mu sync.RWMutex
	hd *h3geodist.Distributed
	ml *Memberlist
}

func NewRouter(
	hd *h3geodist.Distributed,
	ml *Memberlist,
) *Router {
	router := Router{
		hd: hd,
		ml: ml,
	}

	ml.OnJoinFunc(router.handleNodeJoin)
	ml.OnLeaveFunc(router.handleNodeLeave)
	ml.OnUpdateFunc(router.handleNodeUpdate)

	return &router
}

func (r *Router) handleNodeJoin(n Node) {
	_ = r.hd.Add(n.Host())
}

func (r *Router) handleNodeLeave(n Node) {
	r.hd.Remove(n.Host())
}

func (r *Router) handleNodeUpdate(n Node) {

}
