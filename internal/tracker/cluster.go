package tracker

import (
	h3geodist "github.com/mmadfox/go-h3geo-dist"
	"github.com/uber/h3-go/v3"
)

type Cluster interface {
	Lookup(index h3.H3Index) (h3geodist.Cell, bool)
	CurrentLevel() int
	IsOwner(addr string) bool
}

type localCluster struct {
	level int
	dist  *h3geodist.Distributed
	owner string
}

func newLocalCluster(owner string, level int) *localCluster {
	dist, _ := h3geodist.New(level)
	_ = dist.Add(owner)
	return &localCluster{
		dist:  dist,
		level: level,
		owner: owner,
	}
}

func (c *localCluster) AddHost(addr string) {
	_ = c.dist.Add(addr)
}

func (c *localCluster) Lookup(index h3.H3Index) (h3geodist.Cell, bool) {
	return c.dist.Lookup(index)
}

func (c *localCluster) CurrentLevel() int {
	return c.level
}

func (c *localCluster) IsOwner(addr string) bool {
	return c.owner == addr
}

var _ Cluster = (*localCluster)(nil)
