package georule

import (
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

type GeoDetector struct {
	geospatial Geospatial
	vars       Vars
}

type GeoDetectorOption func(*GeoDetector)

func NewGeoDetector(radiusInMeters float64, opts ...GeoDetectorOption) *GeoDetector {
	detector := &GeoDetector{
		geospatial: nearbyGeospatial{
			radiusInMeters: radiusInMeters,
		},
	}
	for _, f := range opts {
		f(detector)
	}
	return detector
}

func WithGeoDetectorVars(v Vars) GeoDetectorOption {
	return func(d *GeoDetector) {
		d.vars = v
	}
}

func WithGeoDetectorGeospatial(g Geospatial) GeoDetectorOption {
	return func(d *GeoDetector) {
		d.geospatial = g
	}
}

type nearbyGeospatial struct {
	radiusInMeters float64
}

func (n nearbyGeospatial) IntersectsPoly(device *Device, object *geojson.Polygon) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (n nearbyGeospatial) IntersectsMultiPoly(device *Device, object *geojson.MultiPolygon) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (n nearbyGeospatial) IntersectsLine(device *Device, object *geojson.LineString) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (n nearbyGeospatial) IntersectsMultiLine(device *Device, object *geojson.MultiLineString) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (n nearbyGeospatial) IntersectsRect(device *Device, object *geojson.Rect) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}

func (n nearbyGeospatial) IntersectsPoint(device *Device, object *geojson.Point) bool {
	return object.IntersectsPoint(geometry.Point{X: device.Latitude, Y: device.Longitude})
}
