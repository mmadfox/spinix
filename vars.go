package georule

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type Vars interface {
	Lookup(ctx context.Context, id string) (interface{}, error)
}

var _ Vars = InMemVars{}

const bucketCount = 64

type InMemVars []*bucket

func NewInMemVars() InMemVars {
	buckets := make(InMemVars, bucketCount)
	for i := 0; i < bucketCount; i++ {
		buckets[i] = &bucket{items: make(map[string]interface{})}
	}
	return buckets
}

func (s InMemVars) bucket(id string) *bucket {
	return s[fnv32(id)%bucketCount]
}

func (s InMemVars) Lookup(_ context.Context, id string) (interface{}, error) {
	if !strings.HasPrefix(id, "@") {
		id = "@" + id
	}
	b := s.bucket(id)
	b.RLock()
	defer b.RUnlock()
	val, ok := b.items[id]
	if !ok {
		return nil, fmt.Errorf("georule/vars: variable %s not found", id)
	}
	return val, nil
}

func (s InMemVars) Set(id string, v interface{}) error {
	if !strings.HasPrefix(id, "@") {
		id = "@" + id
	}
	b := s.bucket(id)
	b.Lock()
	defer b.Unlock()
	_, found := b.items[id]
	if found {
		return fmt.Errorf("geourle/vars: var @%s already exists", id)
	}
	b.items[id] = v
	return nil
}

func (s InMemVars) Remove(id string) {
	if !strings.HasPrefix(id, "@") {
		id = "@" + id
	}
	b := s.bucket(id)
	b.Lock()
	defer b.Unlock()
	delete(b.items, id)
}

func VarsFromSpec(s S) []string {
	vars := make([]string, 0, 8)
	WalkFunc(s.Expr(), func(expr Expr) {
		switch typ := expr.(type) {
		case *CallExpr:
			for _, arg := range typ.Args {
				lit, ok := arg.(*StringLit)
				if !ok {
					continue
				}
				vars = append(vars, lit.Value[1:])
			}
		}
	})
	return vars
}

type bucket struct {
	items map[string]interface{}
	sync.RWMutex
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
