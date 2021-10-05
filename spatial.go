package georule

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
	return geospatial{}
}

type geospatial struct {
}

func (geospatial) IntersectsPoly(device *Device, object *geojson.Polygon) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (geospatial) IntersectsMultiPoly(device *Device, object *geojson.MultiPolygon) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (geospatial) IntersectsLine(device *Device, object *geojson.LineString) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (geospatial) IntersectsMultiLine(device *Device, object *geojson.MultiLineString) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (geospatial) IntersectsRect(device *Device, object *geojson.Rect) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (geospatial) IntersectsPoint(device *Device, object *geojson.Point) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}
