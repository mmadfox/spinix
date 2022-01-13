package cluster

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/mmadfox/spinix/internal/hash"

	"github.com/vmihailenco/msgpack/v5"
)

type Node struct {
	id        uint64
	host      string
	addr      net.Addr
	port      uint16
	hash      uint64
	birthdate int64
}

func NodeFromString(host string) Node {
	birthdate := time.Now().UnixNano()
	return Node{
		id:        host2id(host, birthdate),
		host:      host,
		hash:      hash.StringToUint64(host),
		birthdate: birthdate,
	}
}

func host2id(host string, birthdate int64) uint64 {
	buf := make([]byte, 8+len(host))
	binary.BigEndian.PutUint64(buf, uint64(birthdate))
	buf = append(buf, []byte(host)...)
	return hash.BytesToUint64(buf)
}

func (n Node) ID() uint64 {
	return n.id
}

func (n Node) Host() string {
	return n.host
}

func (n Node) Hash() uint64 {
	return n.hash
}

func (n Node) Birthdate() int64 {
	return n.birthdate
}

func (n Node) String() string {
	return fmt.Sprintf("Node{Host: %s, ID: %d, Hash: %d, Birthdate: %d}",
		n.host, n.id, n.hash, n.birthdate)
}

func EncodeNodeToMeta(n Node) ([]byte, error) {
	return msgpack.Marshal(struct {
		ID        uint64
		Host      string
		Hash      uint64
		Birthdate int64
	}{
		ID:        n.id,
		Host:      n.host,
		Hash:      n.hash,
		Birthdate: n.birthdate,
	})
}

func DecodeNodeFromMeta(meta []byte) (Node, error) {
	n := struct {
		ID        uint64
		Host      string
		Hash      uint64
		Birthdate int64
	}{}
	if err := msgpack.Unmarshal(meta, &n); err != nil {
		return Node{}, err
	}
	return Node{
		id:        n.ID,
		host:      n.Host,
		hash:      n.Hash,
		birthdate: n.Birthdate,
	}, nil
}

func CompareNodeByID(a, b Node) bool {
	return a.ID() == b.ID()
}

func CompareNodeByHost(a, b Node) bool {
	return a.Host() == b.Host()
}

func CompareNodeByHash(a, b Node) bool {
	return a.Hash() == b.Hash()
}
