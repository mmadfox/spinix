package tracker

import (
	"github.com/mmadfox/geojson"
	"github.com/uber/h3-go/v3"
)

type GeoJSON struct {
	ObjectID uint64
	Index    []h3.H3Index
	LayerID  uint64
	Data     geojson.Object
}

func (o GeoJSON) HasIndex() bool {
	return len(o.Index) > 0
}

func (o GeoJSON) HasNotIndex() bool {
	return len(o.Index) == 0
}

func (o GeoJSON) Validate() (err error) {
	if o.ObjectID == 0 {
		err = ErrIdentifierNotDefined
	}
	if !o.Data.Valid() {
		err = ErrInvalidGeoJSONData
	}
	return
}
