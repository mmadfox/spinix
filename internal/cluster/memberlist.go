package cluster

import (
	"fmt"
	"sync"

	"github.com/hashicorp/memberlist"
)

type Memberlist struct {
	config       *memberlist.Config
	owner        Node
	list         *memberlist.Memberlist
	mu           sync.RWMutex
	wg           sync.WaitGroup
	eventsCh     chan memberlist.NodeEvent
	onJoinFunc   []func(Node)
	onLeaveFunc  []func(Node)
	onUpdateFunc []func(Node)
}

func NewMemberlist(c *memberlist.Config) (*Memberlist, error) {
	node := NodeFromString(fmt.Sprintf("%s:%d", c.BindAddr, c.BindPort))
	dg, err := newDelegate(node)
	if err != nil {
		return nil, err
	}
	eventsCh := make(chan memberlist.NodeEvent, 256)
	c.Delegate = dg
	c.Events = &memberlist.ChannelEventDelegate{Ch: eventsCh}
	ml, err := memberlist.Create(c)
	if err != nil {
		return nil, err
	}
	return &Memberlist{
		owner:    node,
		list:     ml,
		config:   c,
		eventsCh: eventsCh,
	}, nil
}

func (c *Memberlist) LocalNode() Node {
	return c.owner
}

func (c *Memberlist) OnLeaveFunc(fn func(Node)) {
	c.onLeaveFunc = append(c.onLeaveFunc, fn)
}

func (c *Memberlist) OnJoinFunc(fn func(Node)) {
	c.onJoinFunc = append(c.onJoinFunc, fn)
}

func (c *Memberlist) OnUpdateFunc(fn func(Node)) {
	c.onUpdateFunc = append(c.onUpdateFunc, fn)
}

func (c *Memberlist) ListenAndServe() error {
	c.wg.Add(1)
	go c.listenAndServe()
	return nil
}

func (c *Memberlist) Join(peers []string) (n int, err error) {
	return c.list.Join(peers)
}

func (c *Memberlist) Shutdown() error {
	return nil
}

func (c *Memberlist) listenAndServe() {
	defer c.wg.Done()
	for {
		select {
		case e := <-c.eventsCh:
			node, err := DecodeNodeFromMeta(e.Node.Meta)
			if err != nil {
				// TODO: add logger
				continue
			}
			switch e.Event {
			case memberlist.NodeLeave:
				for i := 0; i < len(c.onLeaveFunc); i++ {
					c.onLeaveFunc[i](node)
				}
			case memberlist.NodeJoin:
				for i := 0; i < len(c.onJoinFunc); i++ {
					c.onJoinFunc[i](node)
				}
			case memberlist.NodeUpdate:
				for i := 0; i < len(c.onUpdateFunc); i++ {
					c.onUpdateFunc[i](node)
				}
			}
		}
	}
}
