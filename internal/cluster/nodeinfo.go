package cluster

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	clusterv1 "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"
	pb "github.com/mmadfox/spinix/gen/proto/go/cluster/v1"

	"github.com/mmadfox/spinix/internal/hash"
)

type nodeInfo struct {
	id        uint64 // hash from addr + birthdate
	addr      string // GRPC server address
	addrHash  uint64 // hash from addr
	birthdate int64  // unix nano
}

func nodeInfoFromAddr(addr string) nodeInfo {
	birthdate := time.Now().UnixNano()
	return nodeInfo{
		id:        makeNodeID(addr, birthdate),
		addr:      addr,
		addrHash:  hash.StringToUint64(addr),
		birthdate: birthdate,
	}
}

func makeNodeInfo(addr string, port int) nodeInfo {
	return nodeInfoFromAddr(joinAddrPort(addr, port))
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

func (n nodeInfo) ToProto() *pb.NodeInfo {
	return &pb.NodeInfo{
		Id:        n.id,
		Host:      n.addr,
		Hash:      n.addrHash,
		Birthdate: n.birthdate,
	}
}

func encodeNodeInfo(n *nodeInfo) ([]byte, error) {
	return proto.Marshal(&clusterv1.NodeInfo{
		Id:        n.ID(),
		Host:      n.Addr(),
		Hash:      n.AddrHash(),
		Birthdate: n.Birthdate(),
	})
}

func decodeNodeInfo(meta []byte) (nodeInfo, error) {
	ni := clusterv1.NodeInfo{}
	if err := proto.Unmarshal(meta, &ni); err != nil {
		return nodeInfo{}, err
	}
	return nodeInfo{
		id:        ni.GetId(),
		addr:      ni.GetHost(),
		addrHash:  ni.GetHash(),
		birthdate: ni.GetBirthdate(),
	}, nil
}

func compareNodeByID(a, b nodeInfo) bool {
	return a.ID() == b.ID()
}

func compareNodeByAddr(a, b nodeInfo) bool {
	return a.Addr() == b.Addr()
}

func compareNodeByAddrHash(a, b nodeInfo) bool {
	return a.AddrHash() == b.AddrHash()
}
