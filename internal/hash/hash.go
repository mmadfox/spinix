package hash

import (
	"hash/fnv"
)

func BytesToUint64(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

func StringToUint64(b string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(b))
	return h.Sum64()
}
