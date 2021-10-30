package spinix

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/rtree"
)

var ErrDeviceNotFound = errors.New("spinix/devices: device not found")

type Devices interface {
	Lookup(ctx context.Context, deviceID string) (*Device, error)
	InsertOrReplace(ctx context.Context, device *Device) (bool, error)
	Delete(ctx context.Context, deviceID string) error
	Each(ctx context.Context, rid RegionID, size RegionSize, fn func(ctx context.Context, d *Device) error) error
	Nearby(ctx context.Context, lat, lon, meters float64, fn func(ctx context.Context, d *Device) error) error
}

type Device struct {
	IMEI          string  `json:"imei"`
	Owner         string  `json:"owner"`
	Brand         string  `json:"brand"`
	Model         string  `json:"model"`
	Latitude      float64 `json:"lat"`
	Longitude     float64 `json:"lon"`
	Altitude      float64 `json:"alt"`
	Speed         float64 `json:"speed"`
	DateTime      int64   `json:"dateTime"`
	Status        int     `json:"status"`
	BatteryCharge float64 `json:"batteryCharge"`
	Temperature   float64 `json:"temperature"`
	Humidity      float64 `json:"humidity"`
	Luminosity    float64 `json:"luminosity"`
	Pressure      float64 `json:"pressure"`
	FuelLevel     float64 `json:"fuelLevel"`

	regionID RegionID
}

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
	regions map[RegionID]*deviceRegion
	index   deviceIndex
	mu      sync.RWMutex
}

func NewMemoryDevices() Devices {
	return &devices{
		regions: make(map[RegionID]*deviceRegion),
		index:   newDeviceIndex(),
	}
}

func (d *devices) Lookup(_ context.Context, deviceID string) (*Device, error) {
	return d.index.get(deviceID)
}

func (d *devices) Each(
	ctx context.Context,
	rid RegionID, _ RegionSize,
	fn func(ctx context.Context, d *Device) error,
) (err error) {
	d.mu.RLock()
	region, ok := d.regions[rid]
	d.mu.RUnlock()
	if !ok {
		return nil
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
	prevState, err := d.index.get(device.IMEI)
	if prevState != nil && err == nil {
		dist := geo.DistanceTo(
			prevState.Latitude,
			prevState.Longitude,
			device.Latitude,
			device.Longitude,
		)
		if dist <= minDistMeters {
			d.index.set(device)
			replaced = true
			return
		}
		if prevState.RegionID() != device.RegionID() {
			d.index.delete(device.IMEI)
		}
	}
	if err == nil {
		d.mu.RLock()
		region, ok := d.regions[prevState.regionID]
		d.mu.RUnlock()
		if ok {
			replaced = true
			region.delete(prevState)
			if region.isEmpty() {
				d.mu.Lock()
				delete(d.regions, prevState.regionID)
				d.mu.Unlock()
			}
		}
	}
	if errors.Is(err, ErrDeviceNotFound) {
		err = nil
	}
	d.index.set(device)
	d.mu.RLock()
	region, ok := d.regions[device.regionID]
	d.mu.RUnlock()
	if !ok {
		region = newDeviceRegion(device.regionID, TinyRegionSize)
		d.mu.Lock()
		d.regions[device.regionID] = region
		d.mu.Unlock()
	}
	region.insert(device)
	return
}

func (d *devices) Delete(_ context.Context, deviceID string) error {
	prevState, err := d.index.get(deviceID)
	if err != nil {
		return err
	}
	d.mu.RLock()
	region, ok := d.regions[prevState.regionID]
	d.mu.RUnlock()
	if !ok {
		return nil
	}
	region.delete(prevState)
	if region.isEmpty() {
		d.mu.Lock()
		delete(d.regions, prevState.regionID)
		d.mu.Unlock()
	}
	return nil
}

func (d *devices) Nearby(
	ctx context.Context,
	lat, lon, meters float64,
	fn func(ctx context.Context, d *Device) error) (err error) {
	if meters == 0 {
		meters = 1
	} else {
		meters = normalizeDistance(meters, TinyRegionSize)
	}
	ri := regionsFromLatLon(lat, lon, meters, TinyRegionSize)
	points, bbox := makeCircle(lat, lon, meters, steps)
	next := true
	for _, regionID := range ri.regions {
		d.mu.RLock()
		region, found := d.regions[regionID]
		d.mu.RUnlock()
		if !found {
			continue
		}
		region.mu.RLock()
		region.index.Search(
			[2]float64{bbox.Min.X, bbox.Min.Y},
			[2]float64{bbox.Max.X, bbox.Max.Y},
			func(min, max [2]float64, value interface{}) bool {
				device := value.(*Device)
				if contains(geometry.Point{X: device.Latitude, Y: device.Longitude}, points) {
					if err = fn(ctx, device); err != nil {
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
	return
}

type deviceRegion struct {
	id      RegionID
	size    RegionSize
	mu      sync.RWMutex
	devices map[string]*Device
	index   *rtree.RTree
}

func newDeviceRegion(regionID RegionID, size RegionSize) *deviceRegion {
	return &deviceRegion{
		id:      regionID,
		size:    size,
		index:   &rtree.RTree{},
		devices: make(map[string]*Device),
	}
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
	r.index.Insert(
		[2]float64{device.Latitude, device.Longitude},
		[2]float64{device.Latitude, device.Longitude},
		device)
	r.devices[device.IMEI] = device
	r.mu.Unlock()
}

func (r *deviceRegion) delete(device *Device) {
	r.mu.Lock()
	r.index.Delete(
		[2]float64{device.Latitude, device.Longitude},
		[2]float64{device.Latitude, device.Longitude},
		device)
	delete(r.devices, device.IMEI)
	r.mu.Unlock()
}

type deviceIndex []*deviceBucket

type deviceBucket struct {
	sync.RWMutex
	index map[string]*Device
}

func newDeviceIndex() deviceIndex {
	buckets := make([]*deviceBucket, numBucket)
	for i := 0; i < numBucket; i++ {
		buckets[i] = &deviceBucket{
			index: make(map[string]*Device),
		}
	}
	return buckets
}

func (i deviceIndex) bucket(deviceID string) *deviceBucket {
	return i[bucket(deviceID, numBucket)]
}

func (i deviceIndex) set(device *Device) {
	bucket := i.bucket(device.IMEI)
	bucket.Lock()
	bucket.index[device.IMEI] = device
	bucket.Unlock()
}

func (i deviceIndex) delete(deviceID string) {
	bucket := i.bucket(deviceID)
	bucket.Lock()
	delete(bucket.index, deviceID)
	bucket.Unlock()
}

func (i deviceIndex) get(deviceID string) (*Device, error) {
	bucket := i.bucket(deviceID)
	bucket.RLock()
	defer bucket.RUnlock()
	device, ok := bucket.index[deviceID]
	if !ok {
		return nil, fmt.Errorf("%w - %s", ErrDeviceNotFound, deviceID)
	}
	return device, nil
}
