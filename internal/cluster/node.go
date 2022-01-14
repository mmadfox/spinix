package cluster

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	clusterv1 "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"

	"github.com/mmadfox/spinix/internal/hash"
)

type nodeInfo struct {
	id        uint64 // hash from addr + birthdate
	addr      string // GRPC server address
	addrHash  uint64 // hash from addr
	birthdate int64  // unix nano
}

func nodeInfoFromAddr(addr string) *nodeInfo {
	birthdate := time.Now().UnixNano()
	return &nodeInfo{
		id:        makeNodeID(addr, birthdate),
		addr:      addr,
		addrHash:  hash.StringToUint64(addr),
		birthdate: birthdate,
	}
}

func makeNodeID(addr string, birthdate int64) uint64 {
	buf := make([]byte, 8+len(addr))
	binary.BigEndian.PutUint64(buf, uint64(birthdate))
	buf = append(buf, []byte(addr)...)
	return hash.BytesToUint64(buf)
}

func (n nodeInfo) ID() uint64 {
	return n.id
}

func (n nodeInfo) Addr() string {
	return n.addr
}

func (n nodeInfo) AddrHash() uint64 {
	return n.addrHash
}

func (n nodeInfo) Birthdate() int64 {
	return n.birthdate
}

func (n nodeInfo) String() string {
	return fmt.Sprintf("nodeInfo{Addr: %s, ID: %d, AddrHash: %d, Birthdate: %d}",
		n.addr, n.id, n.addrHash, n.birthdate)
}

func encodeNodeInfo(n *nodeInfo) ([]byte, error) {
	return proto.Marshal(&clusterv1.NodeInfo{
		Id:        n.ID(),
		Host:      n.Addr(),
		Hash:      n.AddrHash(),
		Birthdate: n.Birthdate(),
	})
}

func decodeNodeInfo(meta []byte) (*nodeInfo, error) {
	ni := clusterv1.NodeInfo{}
	if err := proto.Unmarshal(meta, &ni); err != nil {
		return nil, err
	}
	return &nodeInfo{
		id:        ni.GetId(),
		addr:      ni.GetHost(),
		addrHash:  ni.GetHash(),
		birthdate: ni.GetBirthdate(),
	}, nil
}

type nodeInfoList struct {
	mu    sync.RWMutex
	store map[uint64]*nodeInfo
}

func newNodeList() *nodeInfoList {
	return &nodeInfoList{store: make(map[uint64]*nodeInfo)}
}

func (nl *nodeInfoList) add(n *nodeInfo) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	nl.store[n.ID()] = n
}

func (nl *nodeInfoList) remove(n *nodeInfo) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	delete(nl.store, n.ID())
}

func (nl *nodeInfoList) removeByAddr(addr string) {
	nl.mu.Lock()
	defer nl.mu.Unlock()
	for id, node := range nl.store {
		if node.Addr() == addr {
			delete(nl.store, id)
		}
	}
}

func compareNodeByID(a, b *nodeInfo) bool {
	return a.ID() == b.ID()
}

func compareNodeByAddr(a, b *nodeInfo) bool {
	return a.Addr() == b.Addr()
}

func compareNodeByAddrHash(a, b *nodeInfo) bool {
	return a.AddrHash() == b.AddrHash()
}
