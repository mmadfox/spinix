package spinix

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/xid"

	"github.com/tidwall/geojson/geo"

	"github.com/tidwall/rtree"

	"github.com/tidwall/geojson/geometry"

	"github.com/tidwall/geojson"
)

var DefaultLayer = xid.NilID()

var ErrObjectNotFound = errors.New("spinix/objects: not found")

type ObjectIterFunc func(ctx context.Context, o *GeoObject) error

type Objects interface {
	Lookup(ctx context.Context, oid ObjectID) (*GeoObject, error)
	Add(ctx context.Context, o *GeoObject) error
	Delete(ctx context.Context, oid ObjectID) error
	Each(ctx context.Context, lid LayerID, rid RegionID, fn ObjectIterFunc) error
	Near(ctx context.Context, lid LayerID, lat, lon, meters float64, fn ObjectIterFunc) error
}

type LayerID = xid.ID
type ObjectID = xid.ID

type GeoObject struct {
	id   ObjectID
	lid  LayerID
	rid  []RegionID
	data geojson.Object
}

func NewGeoObjectWithID(lid LayerID, data geojson.Object) *GeoObject {
	return NewGeoObject(xid.New(), lid, data)
}

func NewGeoObject(oid ObjectID, lid LayerID, data geojson.Object) *GeoObject {
	rect := data.Rect()
	dist := geo.DistanceTo(rect.Min.X, rect.Min.Y, rect.Max.X, rect.Max.Y)
	meters := normalizeDistance(dist/2, SmallRegionSize)
	ri := regionsFromLatLon(rect.Center().X, rect.Center().Y, meters, SmallRegionSize)
	return &GeoObject{
		id:   oid,
		lid:  lid,
		data: data,
		rid:  ri.regions,
	}
}

func (o *GeoObject) RegionSize() RegionSize {
	return SmallRegionSize
}

func (o *GeoObject) RegionID() []RegionID {
	regionIDs := make([]RegionID, len(o.rid))
	copy(regionIDs, o.rid)
	return regionIDs
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

func (o *GeoObject) ID() ObjectID {
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
	bbox := calcRect(lat, lon, meters)
	next := true
	for _, regionID := range ri.regions {
		region, err := o.regionIndex.regionByID(regionID)
		if err != nil {
			continue
		}
		region.mu.RLock()
		region.index.Search(
			[2]float64{bbox.Min.X, bbox.Min.Y},
			[2]float64{bbox.Max.X, bbox.Max.Y},
			func(min, max [2]float64, value interface{}) bool {
				obj := value.(*GeoObject)
				if obj.Layer() != lid {
					return true
				}
				if err = fn(ctx, obj); err != nil {
					next = false
					return false
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

func (o *objects) Lookup(_ context.Context, id ObjectID) (*GeoObject, error) {
	return o.hashIndex.get(id)
}

func (o *objects) Add(_ context.Context, obj *GeoObject) error {
	last, err := o.hashIndex.get(obj.ID())
	if err == nil && last != nil {
		return fmt.Errorf("spinix/objects: object %s already refExists", obj.ID())
	}
	for _, regionID := range obj.RegionID() {
		region, err := o.regionIndex.regionByID(regionID)
		if err != nil {
			region = o.regionIndex.newRegion(regionID)
			if region != nil {
				err = nil
			}
		}
		region.insert(obj)
	}
	o.hashIndex.set(obj)
	return nil
}

func (o *objects) Delete(_ context.Context, id ObjectID) error {
	prevState, err := o.hashIndex.get(id)
	if err != nil {
		return err
	}
	for _, rid := range prevState.RegionID() {
		region, err := o.regionIndex.regionByID(rid)
		// not found
		if err != nil {
			return nil
		}
		region.delete(prevState)
		if region.isEmpty() {
			o.regionIndex.delete(rid)
		}
	}
	o.hashIndex.delete(id)
	return nil
}

type objectHashIndex []*objectBucket

type objectBucket struct {
	sync.RWMutex
	index map[ObjectID]*GeoObject
}

func newObjectsHashIndex() objectHashIndex {
	buckets := make([]*objectBucket, numBucket)
	for i := 0; i < numBucket; i++ {
		buckets[i] = &objectBucket{
			index: make(map[ObjectID]*GeoObject),
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

func (i objectHashIndex) delete(id ObjectID) {
	bucket := i.bucket(id)
	bucket.Lock()
	delete(bucket.index, id)
	bucket.Unlock()
}

func (i objectHashIndex) get(id ObjectID) (*GeoObject, error) {
	bucket := i.bucket(id)
	bucket.RLock()
	defer bucket.RUnlock()
	object, ok := bucket.index[id]
	if !ok {
		return nil, fmt.Errorf("spinix/objects: object %s not found", id)
	}
	return object, nil
}

func (i objectHashIndex) bucket(id ObjectID) *objectBucket {
	return i[bucketFromID(id, numBucket)]
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
	layer map[LayerID]map[ObjectID]*GeoObject
	index *rtree.RTree
}

func newObjectRegion(rid RegionID, size RegionSize) *objectRegion {
	return &objectRegion{
		id:    rid,
		size:  size,
		layer: make(map[LayerID]map[ObjectID]*GeoObject),
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
		o.layer = make(map[LayerID]map[ObjectID]*GeoObject)
	}
}

func (o *objectRegion) renew() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.layer = make(map[LayerID]map[ObjectID]*GeoObject)
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
		o.layer[obj.lid] = make(map[ObjectID]*GeoObject)
	}
	o.layer[obj.lid][obj.ID()] = obj
}
