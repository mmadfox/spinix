package spinix

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/xid"

	"github.com/mmadfox/geojson/geo"
	"github.com/tidwall/rtree"
)

var ErrDeviceNotFound = errors.New("spinix/devices: not found")

type DeviceIterFunc func(ctx context.Context, d *Device) error

type Devices interface {
	Lookup(ctx context.Context, id DeviceID) (*Device, error)
	InsertOrReplace(ctx context.Context, device *Device) (bool, error)
	Delete(ctx context.Context, id DeviceID) error
	Each(ctx context.Context, rid RegionID, size RegionSize, fn DeviceIterFunc) error
	Near(ctx context.Context, lat, lon, meters float64, fn DeviceIterFunc) error
}

type Device struct {
	ID            DeviceID `json:"id"`
	Layer         LayerID  `json:"layerId"`
	IMEI          string   `json:"imei"`
	Owner         string   `json:"owner"`
	Brand         string   `json:"brand"`
	Model         string   `json:"model"`
	Latitude      float64  `json:"lat"`
	Longitude     float64  `json:"lon"`
	Altitude      float64  `json:"alt"`
	Speed         float64  `json:"speed"`
	DateTime      int64    `json:"dateTime"`
	Status        int      `json:"status"`
	BatteryCharge float64  `json:"batteryCharge"`
	Temperature   float64  `json:"temperature"`
	Humidity      float64  `json:"humidity"`
	Luminosity    float64  `json:"luminosity"`
	Pressure      float64  `json:"pressure"`
	FuelLevel     float64  `json:"fuelLevel"`

	regionID RegionID
}

type DeviceID = xid.ID

func (d *Device) DetectRegion() {
	if d.regionID > 0 {
		return
	}
	d.regionID = RegionFromLatLon(d.Latitude, d.Longitude, TinyRegionSize)
}

func (d *Device) ResetRegion() {
	d.regionID = 0
}

func (d *Device) RegionSize() RegionSize {
	return TinyRegionSize
}

func (d *Device) RegionID() RegionID {
	if d.regionID == 0 {
		d.DetectRegion()
	}
	return d.regionID
}

type devices struct {
	hashIndex   deviceHashIndex
	regionIndex *deviceRegionIndex
}

func NewMemoryDevices() Devices {
	return &devices{
		hashIndex:   newDevicesHashIndex(),
		regionIndex: newDevicesRegionIndex(),
	}
}

func (d *devices) Lookup(_ context.Context, id DeviceID) (*Device, error) {
	return d.hashIndex.get(id)
}

func (d *devices) Each(ctx context.Context, rid RegionID, _ RegionSize, fn DeviceIterFunc) (err error) {
	region, err := d.regionIndex.regionByID(rid)
	if err != nil {
		return err
	}
	region.each(func(d *Device) bool {
		err = fn(ctx, d)
		if err != nil {
			return false
		}
		return true
	})
	return
}

func (d *devices) InsertOrReplace(_ context.Context, device *Device) (replaced bool, err error) {
	device.DetectRegion()
	prevState, err := d.hashIndex.get(device.ID)
	if prevState != nil && err == nil {
		dist := geo.DistanceTo(
			prevState.Latitude,
			prevState.Longitude,
			device.Latitude,
			device.Longitude,
		)
		if dist <= minDistMeters {
			d.hashIndex.set(device)
			replaced = true
			return
		}
		if prevState.RegionID() != device.RegionID() {
			d.hashIndex.delete(device.ID)
		}
	}
	if err == nil {
		region, err := d.regionIndex.regionByID(prevState.regionID)
		if err == nil {
			replaced = true
			region.delete(prevState)
			if region.isEmpty() {
				d.regionIndex.delete(prevState.regionID)
			}
		}
	}
	if errors.Is(err, ErrDeviceNotFound) {
		err = nil
	}
	d.hashIndex.set(device)
	region, err := d.regionIndex.regionByID(device.regionID)
	if err != nil {
		region = d.regionIndex.newRegion(device.regionID)
		if region != nil {
			err = nil
		}
	}
	region.insert(device)
	return
}

func (d *devices) Delete(_ context.Context, id DeviceID) error {
	prevState, err := d.hashIndex.get(id)
	if err != nil {
		return err
	}
	region, err := d.regionIndex.regionByID(prevState.regionID)
	// not found
	if err != nil {
		return nil
	}
	region.delete(prevState)
	if region.isEmpty() {
		d.regionIndex.delete(prevState.regionID)
	}
	return nil
}

func (d *devices) Near(ctx context.Context, lat, lon, meters float64, fn DeviceIterFunc) (err error) {
	if meters <= 0 {
		meters = minDistMeters
	} else {
		meters = normalizeDistance(meters, TinyRegionSize)
	}
	ri := regionsFromLatLon(lat, lon, meters, TinyRegionSize)
	bbox := calcRect(lat, lon, meters)
	next := true
	for _, regionID := range ri.regions {
		region, err := d.regionIndex.regionByID(regionID)
		if err != nil {
			continue
		}
		region.mu.RLock()
		region.index.Search(
			[2]float64{bbox.Min.X, bbox.Min.Y},
			[2]float64{bbox.Max.X, bbox.Max.Y},
			func(min, max [2]float64, value interface{}) bool {
				device := value.(*Device)
				if err = fn(ctx, device); err != nil {
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
	return
}

type deviceRegionIndex struct {
	regions map[RegionID]*deviceRegion
	sync.RWMutex
}

func newDevicesRegionIndex() *deviceRegionIndex {
	return &deviceRegionIndex{
		regions: make(map[RegionID]*deviceRegion),
	}
}

func (ri *deviceRegionIndex) newRegion(rid RegionID) (region *deviceRegion) {
	ri.Lock()
	defer ri.Unlock()
	region = newDeviceRegion(rid, TinyRegionSize)
	ri.regions[rid] = region
	return
}

func (ri *deviceRegionIndex) delete(rid RegionID) {
	ri.Lock()
	defer ri.Unlock()
	delete(ri.regions, rid)
}

func (ri *deviceRegionIndex) regionByID(rid RegionID) (*deviceRegion, error) {
	ri.RLock()
	defer ri.RUnlock()
	region, ok := ri.regions[rid]
	if !ok {
		return nil, fmt.Errorf("spinix/devices: region %s not found", rid)
	}
	return region, nil
}

type deviceRegion struct {
	id      RegionID
	size    RegionSize
	mu      sync.RWMutex
	devices map[DeviceID]*Device
	index   *rtree.RTree
}

func newDeviceRegion(regionID RegionID, size RegionSize) *deviceRegion {
	return &deviceRegion{
		id:      regionID,
		size:    size,
		index:   &rtree.RTree{},
		devices: make(map[DeviceID]*Device),
	}
}

func (r *deviceRegion) renew() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.devices = make(map[DeviceID]*Device)
	r.index = &rtree.RTree{}
}

func (r *deviceRegion) isEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.index.Len() == 0
}

func (r *deviceRegion) each(fn func(*Device) bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, d := range r.devices {
		if ok := fn(d); !ok {
			break
		}
	}
}

func (r *deviceRegion) insert(device *Device) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.index.Insert(
		[2]float64{device.Latitude, device.Longitude},
		[2]float64{device.Latitude, device.Longitude},
		device)
	r.devices[device.ID] = device
}

func (r *deviceRegion) delete(device *Device) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.index.Delete(
		[2]float64{device.Latitude, device.Longitude},
		[2]float64{device.Latitude, device.Longitude},
		device)
	delete(r.devices, device.ID)
	if len(r.devices) == 0 {
		r.devices = make(map[DeviceID]*Device)
	}
}

type deviceHashIndex []*deviceBucket

type deviceBucket struct {
	sync.RWMutex
	index map[DeviceID]*Device
}

func newDevicesHashIndex() deviceHashIndex {
	buckets := make([]*deviceBucket, numBucket)
	for i := 0; i < numBucket; i++ {
		buckets[i] = &deviceBucket{
			index: make(map[DeviceID]*Device),
		}
	}
	return buckets
}

func (i deviceHashIndex) bucket(id DeviceID) *deviceBucket {
	return i[bucketFromID(id, numBucket)]
}

func (i deviceHashIndex) set(device *Device) {
	bucket := i.bucket(device.ID)
	bucket.Lock()
	bucket.index[device.ID] = device
	bucket.Unlock()
}

func (i deviceHashIndex) delete(id DeviceID) {
	bucket := i.bucket(id)
	bucket.Lock()
	delete(bucket.index, id)
	bucket.Unlock()
}

func (i deviceHashIndex) get(id DeviceID) (*Device, error) {
	bucket := i.bucket(id)
	bucket.RLock()
	defer bucket.RUnlock()
	device, ok := bucket.index[id]
	if !ok {
		return nil, fmt.Errorf("%w - %s", ErrDeviceNotFound, id)
	}
	return device, nil
}
