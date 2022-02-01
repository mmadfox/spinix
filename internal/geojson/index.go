package geojson

import (
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/geojson/geometry"
	"github.com/uber/h3-go"
)

func EnsureIndex(object geojson.Object, level int) []h3.H3Index {
	if object == nil {
		return []h3.H3Index{}
	}
	if level < 0 {
		level = 0
	}
	if level > 6 {
		level = 6
	}
	unique := make(map[h3.H3Index]struct{})
	object.ForEach(func(geom geojson.Object) bool {
		cells := buildIndex(geom, level)
		for i := 0; i < len(cells); i++ {
			unique[cells[i]] = struct{}{}
		}
		return true
	})
	indexes := make([]h3.H3Index, 0, len(unique))
	for cell := range unique {
		indexes = append(indexes, cell)
	}
	return indexes
}

func buildIndex(o geojson.Object, level int) []h3.H3Index {
	bbox := convertBBOX(o.Rect())
	points := coverBy(bbox, level)
	visit := make(map[h3.H3Index]struct{})
	for i := 0; i < len(points); i++ {
		cellID := h3.FromGeo(h3.GeoCoord{
			Latitude:  points[i].Y,
			Longitude: points[i].X,
		}, level)
		if _, ok := visit[cellID]; ok {
			continue
		}
		visit[cellID] = struct{}{}
	}
	if len(visit) == 0 {
		return []h3.H3Index{}
	}
	cells := make([]h3.H3Index, 0, len(visit))
	for cellID := range visit {
		exterior := make([]geometry.Point, 0, 6)
		boundary := h3.ToGeoBoundary(cellID)
		for _, b := range boundary {
			exterior = append(exterior, geometry.Point{X: b.Longitude, Y: b.Latitude})
		}
		geom := geojson.NewPolygon(geometry.NewPoly(exterior, nil, nil))
		if o.Contains(geom) || o.Intersects(geom) {
			cells = append(cells, cellID)
		}
	}
	return cells
}

func coverBy(bbox geometry.Rect, level int) []geometry.Point {
	edgeMeters := stepFor(level)
	points := make([]geometry.Point, 0, 4)
	seg1Y := bbox.SegmentAt(0)
	seg1X := bbox.SegmentAt(1)
	distX := geo.DistanceTo(seg1X.A.X, seg1X.A.Y, seg1X.B.X, seg1X.B.Y)
	distY := geo.DistanceTo(seg1Y.A.X, seg1Y.A.Y, seg1Y.B.X, seg1Y.B.Y)
	bearingX := geo.BearingTo(seg1X.A.X, seg1X.A.Y, seg1X.B.X, seg1X.B.Y)
	bearingY := geo.BearingTo(seg1Y.A.X, seg1Y.A.Y, seg1Y.B.X, seg1Y.B.Y)
	if distX > edgeMeters {
		for x := float64(0); x < distX; x += edgeMeters {
			xm := x
			if x+edgeMeters > distX {
				xm = distX
			}
			latX, lonX := geo.DestinationPoint(seg1Y.A.X, seg1Y.A.Y, xm, bearingX)
			points = append(points, geometry.Point{X: lonX, Y: latX})
			for y := float64(0); y < distY; y += edgeMeters {
				ym := y
				if y+edgeMeters > distY {
					ym = distY
				}
				latY, lonY := geo.DestinationPoint(latX, lonX, ym, bearingY)
				points = append(points, geometry.Point{X: lonY, Y: latY})
			}
		}
	}
	if distY > edgeMeters {
		for y := float64(0); y < distY; y += edgeMeters {
			ym := y
			if y+edgeMeters > distY {
				ym = distY
			}
			latY, lonY := geo.DestinationPoint(seg1Y.A.X, seg1Y.A.Y, ym, bearingY)
			points = append(points, geometry.Point{X: lonY, Y: latY})
			for x := float64(0); x < distX; x += edgeMeters {
				xm := x
				if x+edgeMeters > distX {
					xm = distX
				}
				latX, lonX := geo.DestinationPoint(latY, lonY, xm, bearingX)
				points = append(points, geometry.Point{X: lonX, Y: latX})
			}
		}
	}
	if len(points) == 0 {
		for i := 0; i < bbox.NumPoints(); i++ {
			p := bbox.PointAt(i)
			points = append(points, geometry.Point{X: p.Y, Y: p.X})
		}
	}
	return points
}

func convertBBOX(src geometry.Rect) (dst geometry.Rect) {
	dst.Min.X = src.Min.Y
	dst.Min.Y = src.Min.X
	dst.Max.X = src.Max.Y
	dst.Max.Y = src.Max.X
	return
}

const (
	level0km = 1107
	level1km = 418
	level2km = 158
	level3km = 59
	level4km = 22
	level5km = 8
	level6km = 3
	level7km = 1
)

var steps = map[int]float64{
	0: level0km,
	1: level1km,
	2: level2km,
	3: level3km,
	4: level4km,
	5: level5km,
	6: level6km,
	7: level7km,
}

func stepFor(level int) (meters float64) {
	v, ok := steps[level]
	if !ok {
		v = level7km
	}
	meters = v * 1000
	return
}
