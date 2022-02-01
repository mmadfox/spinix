package tracker

import "errors"

var (
	ErrIdentifierNotDefined = errors.New("tracker/geojson: identifier not defined")
	ErrNoIndexData          = errors.New("tracker/geojson: no index data")
	ErrInvalidGeoJSONData   = errors.New("tracker/geojson: invalid GeoJSON data")
)
