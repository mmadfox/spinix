package cluster

import (
	"sync"

	"github.com/hashicorp/memberlist"
)

type Cluster struct {
	config     *memberlist.Config
	node       *Node
	memberlist *memberlist.Memberlist
	mu         sync.RWMutex
}

func NewCluster(host string, c *memberlist.Config) (*Cluster, error) {
	node := NodeFromString(host)
	dg, err := newDelegate(node)
	if err != nil {
		return nil, err
	}
	c.Delegate = dg
	c.Events = &memberlist.ChannelEventDelegate{
		Ch: make(chan memberlist.NodeEvent, 256),
	}
	return &Cluster{
		node:   &node,
		config: c,
	}, nil
}

func (c *Cluster) ListenAndServe() error {
	return nil
}

func (c *Cluster) Join(peers []string) (n int, err error) {
	return c.memberlist.Join(peers)
}

func (c *Cluster) Shutdown() error {
	return nil
}
