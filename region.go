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
	TinyRegionSize  RegionSize = 3
	SmallRegionSize RegionSize = 2
	LargeRegionSize RegionSize = 1

	TinyRegionThreshold  = 50000  // meters
	SmallRegionThreshold = 100000 // meters
	LargeRegionThreshold = 300000 // meters
)

const steps = 8

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
	case TinyRegionSize.Value():
		return TinyRegionSize
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

func (rs RegionSize) IsTiny() bool {
	return rs <= TinyRegionThreshold
}

func (rs RegionSize) IsSmall() bool {
	return rs > TinyRegionSize && rs <= SmallRegionThreshold
}

func (rs RegionSize) IsLarge() bool {
	return rs > SmallRegionThreshold && rs <= LargeRegionThreshold
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
	case TinyRegionSize:
		return "tiny"
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

func H3IndexFromLatLon(lat, lon float64, rs RegionSize) h3.H3Index {
	cord := h3.GeoCoord{Latitude: lat, Longitude: lon}
	return h3.FromGeo(cord, rs.Value())
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

func circleFromRule(r *Rule) *geometry.Poly {
	_, points := makeCircle(r.Center().X, r.Center().Y, r.spec.props.radius, steps)
	return &geometry.Poly{Exterior: points}
}

func makeCircle(lat, lng float64, meters float64, steps int) (points []geometry.Point, bbox geometry.Rect) {
	points = make([]geometry.Point, 0, steps+1)
	for i := 0; i <= steps; i++ {
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
			} else if point.Y > bbox.Max.Y {
				bbox.Max.Y = point.Y
			}
		}
	}
	points = append(points, points[0])
	return
}

func normalizeDistance(meters float64, size RegionSize) float64 {
	if meters < 1 {
		meters = 1
	}
	switch {
	case size.IsTiny():
		if meters > TinyRegionThreshold {
			meters = TinyRegionThreshold
		}
	case size.IsSmall():
		if meters > SmallRegionThreshold {
			meters = SmallRegionThreshold
		}
	case size.IsLarge():
		if meters > LargeRegionThreshold {
			meters = LargeRegionThreshold
		}
	}
	return geo.NormalizeDistance(meters)
}

func regionBoundaryFromLatLon(lat, lon float64, size RegionSize) *geometry.Poly {
	index := H3IndexFromLatLon(lat, lon, size)
	boundary := h3.ToGeoBoundary(index)
	points := make([]geometry.Point, len(boundary))
	for i, p := range boundary {
		point := geometry.Point{X: p.Latitude, Y: p.Longitude}
		points[i] = point
	}
	return geometry.NewPoly(points, nil, nil)
}

type regionInfo struct {
	boundary *geometry.Poly
	bbox     geometry.Rect
	regions  []RegionID
}

func regionsFromLatLon(lat, lon, meters float64, size RegionSize) (ri regionInfo) {
	ri.boundary = regionBoundaryFromLatLon(lat, lon, size)
	ri.bbox = calcRect(lat, lon, meters)
	// fast path
	if ri.boundary.ContainsRect(ri.bbox) {
		ri.regions = []RegionID{RegionFromLatLon(lat, lon, size)}
		return
	}
	// slow path
	t := make(map[RegionID]struct{}, 4)
	for i := 0; i <= 3; i++ {
		var p geometry.Point
		b := (i * -360) / steps
		p.X, p.Y = geo.DestinationPoint(lat, lon, meters, float64(b))
		rid := RegionFromLatLon(p.X, p.Y, size)
		_, ok := t[rid]
		if !ok {
			t[rid] = struct{}{}
		}
	}
	ri.regions = make([]RegionID, 0, len(t))
	for rid := range t {
		ri.regions = append(ri.regions, rid)
	}
	return ri
}

func calcRect(lat, lon, meters float64) (rect geometry.Rect) {
	minLat, minLon, maxLat, maxLon := geo.RectFromCenter(lat, lon, meters)
	return geometry.Rect{
		Min: geometry.Point{X: minLat, Y: minLon},
		Max: geometry.Point{X: maxLat, Y: maxLon},
	}
}

func contains(p geometry.Point, points []geometry.Point) bool {
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
