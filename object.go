package spinix

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tidwall/rtree"

	"github.com/tidwall/geojson/geometry"

	"github.com/tidwall/geojson"
)

const DefaultLayer LayerID = "default"

var ErrObjectNotFound = errors.New("spinix/objects: not found")

type ObjectIterFunc func(ctx context.Context, o *GeoObject) error

type Objects interface {
	Lookup(ctx context.Context, objectID string) (*GeoObject, error)
	Add(ctx context.Context, o *GeoObject) error
	Delete(ctx context.Context, objectID string) error
	Each(ctx context.Context, lid LayerID, rid RegionID, fn ObjectIterFunc) error
	Near(ctx context.Context, lid LayerID, lat, lon, meters float64, fn ObjectIterFunc) error
}

type LayerID string

type GeoObject struct {
	id   string
	lid  LayerID
	rid  RegionID
	data geojson.Object
}

func NewGeoObject(id string, lid LayerID, data geojson.Object) *GeoObject {
	center := data.Center()
	regionID := RegionFromLatLon(center.X, center.Y, SmallRegionSize)
	return &GeoObject{
		id:   id,
		lid:  lid,
		data: data,
		rid:  regionID,
	}
}

func (o *GeoObject) RegionSize() RegionSize {
	return SmallRegionSize
}

func (o *GeoObject) RegionID() RegionID {
	return o.rid
}

func (o *GeoObject) Within(other *GeoObject) bool {
	return o.data.Within(other.data)
}

func (o *GeoObject) Intersects(other *GeoObject) bool {
	return o.data.Intersects(other.data)
}

func (o *GeoObject) Contains(other *GeoObject) bool {
	return o.data.Contains(other.data)
}

func (o *GeoObject) Center() geometry.Point {
	return o.data.Center()
}

func (o *GeoObject) Boundary() geometry.Rect {
	return o.data.Rect()
}

func (o *GeoObject) Layer() LayerID {
	return o.lid
}

func (o *GeoObject) ID() string {
	return o.id
}

func (o *GeoObject) Data() geojson.Object {
	return o.data
}

func NewMemoryObjects() Objects {
	return &objects{
		hashIndex:   newObjectsHashIndex(),
		regionIndex: newObjectRegionIndex(),
	}
}

type objects struct {
	hashIndex   objectHashIndex
	regionIndex *objectRegionIndex
}

func (o *objects) Near(ctx context.Context, lid LayerID, lat, lon, meters float64, fn ObjectIterFunc) error {
	if meters <= 0 {
		meters = minDistMeters
	} else {
		meters = normalizeDistance(meters, SmallRegionSize)
	}
	ri := regionsFromLatLon(lat, lon, meters, SmallRegionSize)
	ring := makeRadiusRing(lat, lon, meters, steps)
	circle := geojson.NewPolygon(&geometry.Poly{Exterior: ring})
	next := true
	for _, regionID := range ri.regions {
		region, err := o.regionIndex.regionByID(regionID)
		if err != nil {
			continue
		}
		region.mu.RLock()
		region.index.Search(
			[2]float64{ring.rect.Min.X, ring.rect.Min.Y},
			[2]float64{ring.rect.Max.X, ring.rect.Max.Y},
			func(min, max [2]float64, value interface{}) bool {
				obj := value.(*GeoObject)
				if circle.Contains(obj.Data()) && obj.Layer() == lid {
					if err = fn(ctx, obj); err != nil {
						next = false
						return false
					}
				}
				return true
			},
		)
		region.mu.RUnlock()
		if !next {
			break
		}
	}
	return nil
}

func (o *objects) Each(ctx context.Context, lid LayerID, rid RegionID, fn ObjectIterFunc) error {
	region, err := o.regionIndex.regionByID(rid)
	if err != nil {
		return err
	}
	region.each(lid, func(obj *GeoObject) bool {
		err = fn(ctx, obj)
		if err != nil {
			return false
		}
		return true
	})
	return nil
}

func (o *objects) Lookup(_ context.Context, objectID string) (*GeoObject, error) {
	return o.hashIndex.get(objectID)
}

func (o *objects) Add(_ context.Context, obj *GeoObject) error {
	region, err := o.regionIndex.regionByID(obj.RegionID())
	if err != nil {
		region = o.regionIndex.newRegion(obj.RegionID())
		if region != nil {
			err = nil
		}
	}
	region.insert(obj)
	o.hashIndex.set(obj)
	return nil
}

func (o *objects) Delete(_ context.Context, objectID string) error {
	prevState, err := o.hashIndex.get(objectID)
	if err != nil {
		return err
	}
	region, err := o.regionIndex.regionByID(prevState.RegionID())
	// not found
	if err != nil {
		return nil
	}
	region.delete(prevState)
	if region.isEmpty() {
		o.regionIndex.delete(prevState.RegionID())
	}
	o.hashIndex.delete(objectID)
	return nil
}

type objectHashIndex []*objectBucket

type objectBucket struct {
	sync.RWMutex
	index map[string]*GeoObject
}

func newObjectsHashIndex() objectHashIndex {
	buckets := make([]*objectBucket, numBucket)
	for i := 0; i < numBucket; i++ {
		buckets[i] = &objectBucket{
			index: make(map[string]*GeoObject),
		}
	}
	return buckets
}

func (i objectHashIndex) set(obj *GeoObject) {
	bucket := i.bucket(obj.ID())
	bucket.Lock()
	bucket.index[obj.ID()] = obj
	bucket.Unlock()
}

func (i objectHashIndex) delete(objectID string) {
	bucket := i.bucket(objectID)
	bucket.Lock()
	delete(bucket.index, objectID)
	bucket.Unlock()
}

func (i objectHashIndex) get(objectID string) (*GeoObject, error) {
	bucket := i.bucket(objectID)
	bucket.RLock()
	defer bucket.RUnlock()
	object, ok := bucket.index[objectID]
	if !ok {
		return nil, fmt.Errorf("spinix/objects: object %s not found", objectID)
	}
	return object, nil
}

func (i objectHashIndex) bucket(objectID string) *objectBucket {
	return i[bucket(objectID, numBucket)]
}

type objectRegionIndex struct {
	regions map[RegionID]*objectRegion
	sync.RWMutex
}

func newObjectRegionIndex() *objectRegionIndex {
	return &objectRegionIndex{
		regions: make(map[RegionID]*objectRegion),
	}
}

func (ri *objectRegionIndex) newRegion(rid RegionID) (region *objectRegion) {
	ri.Lock()
	defer ri.Unlock()
	region = newObjectRegion(rid, SmallRegionSize)
	ri.regions[rid] = region
	return
}

func (ri *objectRegionIndex) delete(rid RegionID) {
	ri.Lock()
	defer ri.Unlock()
	delete(ri.regions, rid)
}

func (ri *objectRegionIndex) regionByID(rid RegionID) (*objectRegion, error) {
	ri.RLock()
	defer ri.RUnlock()
	region, ok := ri.regions[rid]
	if !ok {
		return nil, fmt.Errorf("spinix/objects: region %s not found", rid)
	}
	return region, nil
}

type objectRegion struct {
	id    RegionID
	size  RegionSize
	mu    sync.RWMutex
	layer map[LayerID]map[string]*GeoObject
	index *rtree.RTree
}

func newObjectRegion(rid RegionID, size RegionSize) *objectRegion {
	return &objectRegion{
		id:    rid,
		size:  size,
		layer: make(map[LayerID]map[string]*GeoObject),
		index: &rtree.RTree{},
	}
}

func (o *objectRegion) isEmpty() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.index.Len() == 0
}

func (o *objectRegion) delete(obj *GeoObject) {
	o.mu.Lock()
	defer o.mu.Unlock()
	bbox := obj.Boundary()
	o.index.Delete(
		[2]float64{bbox.Min.X, bbox.Min.Y},
		[2]float64{bbox.Max.X, bbox.Max.Y},
		obj)
	delete(o.layer[obj.lid], obj.id)
	if len(o.layer[obj.lid]) == 0 {
		o.layer = make(map[LayerID]map[string]*GeoObject)
	}
}

func (o *objectRegion) renew() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.layer = make(map[LayerID]map[string]*GeoObject)
}

func (o *objectRegion) each(lid LayerID, fn func(*GeoObject) bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	objects, found := o.layer[lid]
	if !found {
		return
	}
	for _, d := range objects {
		if ok := fn(d); !ok {
			break
		}
	}
}

func (o *objectRegion) insert(obj *GeoObject) {
	o.mu.Lock()
	defer o.mu.Unlock()
	bbox := obj.Boundary()
	o.index.Insert(
		[2]float64{bbox.Min.X, bbox.Min.Y},
		[2]float64{bbox.Max.X, bbox.Max.Y},
		obj)
	if o.layer[obj.lid] == nil {
		o.layer[obj.lid] = make(map[string]*GeoObject)
	}
	o.layer[obj.lid][obj.ID()] = obj
}
