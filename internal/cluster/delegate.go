package cluster

import "github.com/hashicorp/memberlist"

// delegate is a struct which implements memberlist.Delegate interface.
type delegate struct {
	meta []byte
}

var _ memberlist.Delegate = (*delegate)(nil)

func newDelegate(n *Node) (delegate, error) {
	data, err := encodeNodeToMeta(n)
	if err != nil {
		return delegate{}, err
	}
	return delegate{meta: data}, nil
}

func (d delegate) NodeMeta(_ int) []byte {
	return d.meta
}

func (d delegate) NotifyMsg(_ []byte) {}

func (d delegate) GetBroadcasts(_, _ int) [][]byte { return nil }

func (d delegate) LocalState(_ bool) []byte { return nil }

func (d delegate) MergeRemoteState(_ []byte, _ bool) {}
