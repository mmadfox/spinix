package cluster

import (
	"sync"

	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
)

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

func (v *vnode) NoData() bool {
	return true
}

func (v *vnode) SetOwners(owners []*pb.NodeInfo) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.owners = make([]nodeInfo, len(owners))
	for i := 0; i < len(owners); i++ {
		owner := owners[i]
		v.owners[i] = nodeInfo{
			id:        owner.GetId(),
			addr:      owner.GetHost(),
			addrHash:  owner.GetHash(),
			birthdate: owner.GetBirthdate(),
		}
	}
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
