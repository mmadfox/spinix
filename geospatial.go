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

	WithinPoint(device *Device, object *geojson.Point) bool
	WithinLine(device *Device, object *geojson.LineString) bool
	WithinMultiLine(device *Device, object *geojson.MultiLineString) bool
	WithinPoly(device *Device, object *geojson.Polygon) bool
	WithinRect(device *Device, object *geojson.Rect) bool
	WithinMultiPoly(device *Device, object *geojson.MultiPolygon) bool
}

func DefaultGeospatial() Geospatial {
	return defaultGeospatial{}
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

func (defaultGeospatial) WithinPoint(device *Device, object *geojson.Point) bool {
	return object.WithinPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (defaultGeospatial) WithinLine(device *Device, object *geojson.LineString) bool {
	return object.WithinPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (defaultGeospatial) WithinMultiLine(device *Device, object *geojson.MultiLineString) bool {
	return object.WithinPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (defaultGeospatial) WithinPoly(device *Device, object *geojson.Polygon) bool {
	point := geojson.NewPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
	return point.WithinPoly(object.Base())
}

func (defaultGeospatial) WithinMultiPoly(device *Device, object *geojson.MultiPolygon) bool {
	point := geojson.NewPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
	return point.Within(object)
}

func (defaultGeospatial) WithinRect(device *Device, object *geojson.Rect) bool {
	return object.WithinPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}
