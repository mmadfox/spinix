package cluster

import "sync"

type VNodeKind int

const (
	Primary VNodeKind = iota + 1
	Secondary
)

type vnode struct {
	id     uint64
	kind   VNodeKind
	mu     sync.RWMutex
	owners []nodeInfo
}

type vnodeList struct {
	count  uint64
	kind   VNodeKind
	vnodes map[uint64]*vnode
}

func newVNodeList(count uint64, kind VNodeKind) *vnodeList {
	vl := &vnodeList{
		count:  count,
		kind:   kind,
		vnodes: make(map[uint64]*vnode),
	}
	for i := uint64(0); i < count; i++ {
		vl.vnodes[i] = &vnode{id: i, kind: kind, owners: make([]nodeInfo, 0)}
	}
	return vl
}

func (vl *vnodeList) ByID(vnode uint64) *vnode {
	return vl.vnodes[vnode]
}
