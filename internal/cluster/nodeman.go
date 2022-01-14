package cluster

import (
	"fmt"
	"sort"
	"sync"

	"github.com/hashicorp/memberlist"
)

type nodeman struct {
	owner        *nodeInfo
	config       *memberlist.Config
	list         *memberlist.Memberlist
	mu           sync.RWMutex
	wg           sync.WaitGroup
	stopCh       chan struct{}
	eventsCh     chan memberlist.NodeEvent
	onJoinFunc   []func(*nodeInfo)
	onLeaveFunc  []func(*nodeInfo)
	onUpdateFunc []func(*nodeInfo)
	onChangeFunc []func()
}

func nodemanFromMemberlistConfig(owner *nodeInfo, c *memberlist.Config) (*nodeman, error) {
	eventsCh := make(chan memberlist.NodeEvent, 256)
	c.Events = &memberlist.ChannelEventDelegate{Ch: eventsCh}
	c.Delegate = newDelegate(owner)
	return &nodeman{config: c, eventsCh: eventsCh, owner: owner}, nil
}

func (c *nodeman) Nodes() ([]*nodeInfo, error) {
	members := c.list.Members()
	nodes := make([]*nodeInfo, 0, len(members))
	for i := 0; i < len(members); i++ {
		node, err := decodeNodeInfo(members[i].Meta)
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

func (c *nodeman) OnLeaveFunc(fn func(*nodeInfo)) {
	c.onLeaveFunc = append(c.onLeaveFunc, fn)
}

func (c *nodeman) OnJoinFunc(fn func(*nodeInfo)) {
	c.onJoinFunc = append(c.onJoinFunc, fn)
}

func (c *nodeman) OnUpdateFunc(fn func(*nodeInfo)) {
	c.onUpdateFunc = append(c.onUpdateFunc, fn)
}

func (c *nodeman) OnChangeFunc(fn func()) {
	c.onChangeFunc = append(c.onChangeFunc, fn)
}

func (c *nodeman) Owner() *nodeInfo {
	return c.owner
}

func (c *nodeman) Coordinator() (*nodeInfo, error) {
	nodes, err := c.Nodes()
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("cluster/memberlist: there is no nodeInfo in memberlist")
	}
	oldest := nodes[0]
	return oldest, nil
}

func (c *nodeman) IsCoordinator() bool {
	oldest, err := c.Coordinator()
	if err != nil {
		return false
	}
	return oldest.ID() == c.owner.ID()
}

func (c *nodeman) ListenAndServe() error {
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

func (c *nodeman) Join(peers []string) (n int, err error) {
	if c.list == nil {
		return -1, fmt.Errorf("cluster/memberlist: first run the memberlist and then join peers")
	}
	return c.list.Join(peers)
}

func (c *nodeman) NumNodes() int {
	return c.list.NumMembers()
}

func (c *nodeman) Shutdown() error {
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

func (c *nodeman) isClosed() bool {
	select {
	case <-c.stopCh:
		return true
	default:
	}
	return false
}

func (c *nodeman) dispatchEvents() {
	defer c.wg.Done()
	for {
		select {
		case <-c.stopCh:
			return
		case e := <-c.eventsCh:
			node, err := decodeNodeInfo(e.Node.Meta)
			if err != nil {
				continue
			}
			if compareNodeByAddr(c.owner, node) {
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

// delegate is a struct which implements memberlist.Delegate interface.
type delegate struct {
	meta []byte
}

var _ memberlist.Delegate = (*delegate)(nil)

func newDelegate(n *nodeInfo) delegate {
	data, _ := encodeNodeInfo(n)
	return delegate{meta: data}
}

func (d delegate) NodeMeta(_ int) []byte {
	return d.meta
}

func (d delegate) NotifyMsg(_ []byte) {}

func (d delegate) GetBroadcasts(_, _ int) [][]byte { return nil }

func (d delegate) LocalState(_ bool) []byte { return nil }

func (d delegate) MergeRemoteState(_ []byte, _ bool) {}
