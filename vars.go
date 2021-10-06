package georule

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type Vars interface {
	Lookup(ctx context.Context, id string) (interface{}, error)
	Set(ctx context.Context, id string, v interface{}) error
	Remove(ctx context.Context, id string) error
}

var _ Vars = InMemVars{}

const bucketCount = 64

type InMemVars []*varBucket

func NewInMemVars() InMemVars {
	buckets := make(InMemVars, bucketCount)
	for i := 0; i < bucketCount; i++ {
		buckets[i] = &varBucket{items: make(map[string]interface{})}
	}
	return buckets
}

func (s InMemVars) bucket(id string) *varBucket {
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

func (s InMemVars) Set(_ context.Context, id string, v interface{}) error {
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

func (s InMemVars) Remove(_ context.Context, id string) error {
	if !strings.HasPrefix(id, "@") {
		id = "@" + id
	}
	b := s.bucket(id)
	b.Lock()
	defer b.Unlock()
	delete(b.items, id)
	return nil
}

func VarsFromSpec(s S) map[string]struct{} {
	vars := make(map[string]struct{})
	WalkFunc(s.Expr(), func(expr Expr) {
		switch typ := expr.(type) {
		case *CallExpr:
			for _, arg := range typ.Args {
				lit, ok := arg.(*StringLit)
				if !ok {
					continue
				}
				vars[lit.Value[1:]] = struct{}{}
			}
		}
	})
	return vars
}

type varBucket struct {
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
