package cluster

import (
	"sync"

	h3geodist "github.com/mmadfox/go-h3geo-dist"
)

type router struct {
	mu  sync.RWMutex
	hd  *h3geodist.Distributed
	nl  *nodeInfoList
	cli *client
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
