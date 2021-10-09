package spinix

import (
	"context"

	"github.com/tidwall/geojson"
)

type ObjectLookup interface {
	Lookup(ctx context.Context, objectID string) (geojson.Object, error)
}

type Objects interface {
	ObjectLookup

	Set(ctx context.Context, objectID string, o geojson.Object) error
	Delete(ctx context.Context, objectID string, o geojson.Object) error
}
