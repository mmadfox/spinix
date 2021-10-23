package spinix

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tidwall/geojson"
)

var ErrObjectNotFound = errors.New("spinix/objects: object not found")

type Objects interface {
	Lookup(ctx context.Context, objectID string) (geojson.Object, error)
	Add(ctx context.Context, objectID string, o geojson.Object) error
	Delete(ctx context.Context, objectID string) error
}

func NewMemoryObjects() Objects {
	return &objects{
		index: newObjectIndex(),
	}
}

type objects struct {
	index objectIndex
}

func (o *objects) Lookup(_ context.Context, objectID string) (geojson.Object, error) {
	return o.index.get(objectID)
}

func (o *objects) Add(_ context.Context, objectID string, obj geojson.Object) error {
	o.index.set(objectID, obj)
	return nil
}

func (o *objects) Delete(_ context.Context, objectID string) error {
	o.index.delete(objectID)
	return nil
}

type objectIndex []*objectBucket

type objectBucket struct {
	sync.RWMutex
	index map[string]geojson.Object
}

func newObjectIndex() objectIndex {
	buckets := make([]*objectBucket, numBucket)
	for i := 0; i < numBucket; i++ {
		buckets[i] = &objectBucket{
			index: make(map[string]geojson.Object),
		}
	}
	return buckets
}

func (i objectIndex) set(objectID string, object geojson.Object) {
	bucket := i.bucket(objectID)
	bucket.Lock()
	bucket.index[objectID] = object
	bucket.Unlock()
}

func (i objectIndex) delete(objectID string) {
	bucket := i.bucket(objectID)
	bucket.Lock()
	delete(bucket.index, objectID)
	bucket.Unlock()
}

func (i objectIndex) get(objectID string) (geojson.Object, error) {
	bucket := i.bucket(objectID)
	bucket.RLock()
	defer bucket.RUnlock()
	object, ok := bucket.index[objectID]
	if !ok {
		return nil, fmt.Errorf("spinix: object %s not found", objectID)
	}
	return object, nil
}

func (i objectIndex) bucket(objectID string) *objectBucket {
	return i[bucket(objectID, numBucket)]
}
