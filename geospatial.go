package spinix

import (
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

type Geospatial interface {
	IntersectsPoly(device *Device, object *geojson.Polygon) bool
	IntersectsMultiPoly(device *Device, object *geojson.MultiPolygon) bool
	IntersectsLine(device *Device, object *geojson.LineString) bool
	IntersectsMultiLine(device *Device, object *geojson.MultiLineString) bool
	IntersectsRect(device *Device, object *geojson.Rect) bool
	IntersectsPoint(device *Device, object *geojson.Point) bool
}

func DefaultGeospatial() Geospatial {
	return defaultGeospatial{}
}

func Rect(min, max geometry.Point) *geojson.Rect {
	return geojson.NewRect(geometry.Rect{Min: min, Max: max})
}

func Poly(exterior []geometry.Point, holes [][]geometry.Point) *geojson.Polygon {
	return geojson.NewPolygon(geometry.NewPoly(exterior, holes, nil))
}

func Point(x, y float64) *geojson.Point {
	return geojson.NewPoint(geometry.Point{X: x, Y: y})
}

type defaultGeospatial struct {
}

func (defaultGeospatial) IntersectsPoly(device *Device, object *geojson.Polygon) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (defaultGeospatial) IntersectsMultiPoly(device *Device, object *geojson.MultiPolygon) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (defaultGeospatial) IntersectsLine(device *Device, object *geojson.LineString) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (defaultGeospatial) IntersectsMultiLine(device *Device, object *geojson.MultiLineString) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (defaultGeospatial) IntersectsRect(device *Device, object *geojson.Rect) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (defaultGeospatial) IntersectsPoint(device *Device, object *geojson.Point) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}
