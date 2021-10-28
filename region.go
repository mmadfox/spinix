package spinix

import (
	"context"
	"fmt"
	"sync"

	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/geojson/geometry"
	"github.com/tidwall/rtree"
	"github.com/uber/h3-go"
)

const (
	SmallRegionSize      RegionSize = 3
	LargeRegionSize      RegionSize = 1
	SmallRegionThreshold            = 20000  // meters
	LargeRegionThreshold            = 350000 // meters
	Steps                           = 16
)

type Region struct {
	id       RegionID
	size     RegionSize
	center   geometry.Point
	bounding []geometry.Point
}

func MakeRegion(id RegionID, size RegionSize) Region {
	h3index := h3.H3Index(id)
	cord := h3.ToGeo(h3index)
	boundary := h3.ToGeoBoundary(h3index)
	region := Region{
		id:       id,
		size:     size,
		center:   geometry.Point{X: cord.Latitude, Y: cord.Longitude},
		bounding: make([]geometry.Point, len(boundary)),
	}
	for i, b := range boundary {
		region.bounding[i] = geometry.Point{X: b.Latitude, Y: b.Longitude}
	}
	return region
}

func (r Region) String() string {
	return fmt.Sprintf("Region{ID:%d, Size:%s, Center:%v}", r.id, r.size, r.center)
}

func (r Region) ID() RegionID {
	return r.id
}

func (r Region) Size() RegionSize {
	return r.size
}

func (r Region) Center() geometry.Point {
	return r.center
}

func (r Region) Bounding() []geometry.Point {
	points := make([]geometry.Point, len(r.bounding))
	copy(points, r.bounding)
	return points
}

type RegionID uint64

func RegionIDFromString(id string) (RegionID, error) {
	if len(id) == 0 {
		return 0, fmt.Errorf("spinix/region: got empty region id")
	}
	return RegionID(h3.FromString(id)), nil
}

func (rid RegionID) String() string {
	return h3.ToString(h3.H3Index(rid))
}

func (rid RegionID) Size() RegionSize {
	res := h3.Resolution(h3.H3Index(rid))
	switch res {
	case SmallRegionSize.Value():
		return SmallRegionSize
	case LargeRegionSize.Value():
		return LargeRegionSize
	default:
		return RegionSize(-1)
	}
}

type RegionSize int

func (rs RegionSize) Validate() (err error) {
	if rs > LargeRegionThreshold {
		err = fmt.Errorf("spinix/region: region size too large")
	}
	return
}

func (rs RegionSize) IsSmall() bool {
	return rs <= SmallRegionThreshold
}

func (rs RegionSize) IsLarge() bool {
	return rs > SmallRegionThreshold && rs < LargeRegionThreshold
}

func (rs RegionSize) Threshold() float64 {
	switch rs {
	case SmallRegionSize:
		return SmallRegionThreshold
	case LargeRegionSize:
		return LargeRegionThreshold
	default:
		return 0
	}
}

func (rs RegionSize) Value() int {
	return int(rs)
}

func (rs RegionSize) String() string {
	switch rs {
	case SmallRegionSize:
		return "small"
	case LargeRegionSize:
		return "large"
	default:
		return "unknown region size"
	}
}

type regionCell struct {
	id   RegionID
	size RegionSize
	sync.RWMutex
	index *rtree.RTree
}

func newRegionCell(rid RegionID, size RegionSize) *regionCell {
	return &regionCell{
		id:    rid,
		size:  size,
		index: &rtree.RTree{},
	}
}

func (sr *regionCell) isEmpty() bool {
	sr.RLock()
	defer sr.RUnlock()
	return sr.index.Len() == 0
}

func (sr *regionCell) insert(r *Rule) {
	sr.Lock()
	defer sr.Unlock()
	bbox := r.Bounding()
	sr.index.Insert(
		[2]float64{bbox.Min.X, bbox.Min.Y},
		[2]float64{bbox.Max.X, bbox.Max.Y},
		r,
	)
}

func (sr *regionCell) delete(r *Rule) {
	sr.Lock()
	defer sr.Unlock()
	bbox := r.Bounding()
	sr.index.Delete(
		[2]float64{bbox.Min.X, bbox.Min.Y},
		[2]float64{bbox.Max.X, bbox.Max.Y},
		r,
	)
}

func (sr *regionCell) walk(ctx context.Context, lat float64, lon float64, fn WalkRuleFunc) (err error) {
	sr.RLock()
	defer sr.RUnlock()
	sr.index.Search(
		[2]float64{lat, lon},
		[2]float64{lat, lon},
		func(_, _ [2]float64, value interface{}) bool {
			rule, ok := value.(*Rule)
			if ok {
				if err = fn(ctx, rule, nil); err != nil {
					return false
				}
			}
			return true
		},
	)
	return
}

func RegionSizeFromMeters(value float64) RegionSize {
	if value <= SmallRegionThreshold {
		return SmallRegionSize
	} else {
		return LargeRegionSize
	}
}

func RegionFromLatLon(lat, lon float64, rs RegionSize) RegionID {
	cord := h3.GeoCoord{Latitude: lat, Longitude: lon}
	return RegionID(h3.FromGeo(cord, rs.Value()))
}

func RegionIDs(points []geometry.Point, rs RegionSize) []RegionID {
	visits := make(map[RegionID]struct{})
	res := make([]RegionID, 0, 3)
	for _, p := range points {
		idx := RegionFromLatLon(p.X, p.Y, rs)
		_, visit := visits[idx]
		if !visit {
			res = append(res, idx)
			visits[idx] = struct{}{}
		}
	}
	return res
}

func MakeCircle(lat, lng float64, meters float64, steps int) (points []geometry.Point, bbox geometry.Rect) {
	meters = geo.NormalizeDistance(meters)
	points = make([]geometry.Point, 0, steps+1)
	for i := 0; i < steps; i++ {
		b := (i * -360) / steps
		lat, lng := geo.DestinationPoint(lat, lng, meters, float64(b))
		point := geometry.Point{X: lat, Y: lng}
		points = append(points, point)
		if i == 0 {
			bbox.Min = point
			bbox.Max = point
		} else {
			if point.X < bbox.Min.X {
				bbox.Min.X = point.X
			} else if point.X > bbox.Max.X {
				bbox.Max.X = point.X
			}
			if point.Y < bbox.Min.Y {
				bbox.Min.Y = point.Y
			} else if points[i].Y > bbox.Max.Y {
				bbox.Max.Y = points[i].Y
			}
		}
	}
	points = append(points, points[0])
	return
}

func Contains(p geometry.Point, points []geometry.Point) bool {
	for i := 0; i < len(points); i++ {
		var seg geometry.Segment
		seg.A = points[i]
		if i == len(points)-1 {
			seg.B = points[0]
		} else {
			seg.B = points[i+1]
		}
		if seg.ContainsPoint(p) {
			return true
		}
		res := seg.Raycast(p)
		if res.In {
			return true
		}
	}
	return false
}
