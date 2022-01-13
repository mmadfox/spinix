package cluster

import (
	"fmt"
	"sort"
	"sync"

	"github.com/hashicorp/memberlist"
)

type Memberlist struct {
	config       *memberlist.Config
	owner        *Node
	list         *memberlist.Memberlist
	mu           sync.RWMutex
	wg           sync.WaitGroup
	stopCh       chan struct{}
	eventsCh     chan memberlist.NodeEvent
	onJoinFunc   []func(*Node)
	onLeaveFunc  []func(*Node)
	onUpdateFunc []func(*Node)
	onChangeFunc []func()
}

func NewMemberlist(c *memberlist.Config) (*Memberlist, error) {
	if c == nil {
		return nil, fmt.Errorf("cluster/memberlist: config nil pointer")
	}
	name := fmt.Sprintf("%s:%d", c.BindAddr, c.BindPort)
	owner := nodeFromString(name)
	dg, err := newDelegate(owner)
	if err != nil {
		return nil, err
	}
	eventsCh := make(chan memberlist.NodeEvent, 256)
	c.Delegate = dg
	c.Events = &memberlist.ChannelEventDelegate{Ch: eventsCh}
	c.Name = name
	return &Memberlist{
		owner:    owner,
		config:   c,
		eventsCh: eventsCh,
	}, nil
}

func (c *Memberlist) Owner() *Node {
	return c.owner
}

func (c *Memberlist) Nodes() ([]*Node, error) {
	members := c.list.Members()
	nodes := make([]*Node, 0, len(members))
	for i := 0; i < len(members); i++ {
		node, err := decodeNodeFromMeta(members[i].Meta)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i int, j int) bool {
		return nodes[i].Birthdate() < nodes[j].Birthdate()
	})
	return nodes, nil
}

func (c *Memberlist) OnLeaveFunc(fn func(*Node)) {
	c.onLeaveFunc = append(c.onLeaveFunc, fn)
}

func (c *Memberlist) OnJoinFunc(fn func(*Node)) {
	c.onJoinFunc = append(c.onJoinFunc, fn)
}

func (c *Memberlist) OnUpdateFunc(fn func(*Node)) {
	c.onUpdateFunc = append(c.onUpdateFunc, fn)
}

func (c *Memberlist) OnChangeFunc(fn func()) {
	c.onChangeFunc = append(c.onChangeFunc, fn)
}

func (c *Memberlist) ListenAndServe() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ml, err := memberlist.Create(c.config)
	if err != nil {
		return err
	}
	c.list = ml
	c.stopCh = make(chan struct{})
	c.wg.Add(1)
	go c.dispatchEvents()
	return nil
}

func (c *Memberlist) Join(peers []string) (n int, err error) {
	if c.list == nil {
		return -1, fmt.Errorf("cluster/memberlist: first run the memberlist and then join peers")
	}
	return c.list.Join(peers)
}

func (c *Memberlist) Shutdown() error {
	if c.list == nil {
		return nil
	}
	if c.isClosed() {
		return nil
	}
	close(c.stopCh)
	c.wg.Wait()
	return c.list.Shutdown()
}

func (c *Memberlist) isClosed() bool {
	select {
	case <-c.stopCh:
		return true
	default:
	}
	return false
}

func (c *Memberlist) dispatchEvents() {
	defer c.wg.Done()
	for {
		select {
		case <-c.stopCh:
			return
		case e := <-c.eventsCh:
			node, err := decodeNodeFromMeta(e.Node.Meta)
			if err != nil {
				continue
			}
			if compareNodeByHost(c.owner, node) {
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
			for i := 0; i < len(c.onChangeFunc); i++ {
				c.onChangeFunc[i]()
			}
		}
	}
}
