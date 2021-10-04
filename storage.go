package georule

import (
	"context"

	"github.com/tidwall/geojson"
)

type VarStorage interface {
	Lookup(ctx context.Context, id string) (geojson.Object, error)
	Set(ctx context.Context, id string, object geojson.Object) error
	Delete(ctx context.Context, id string) error
}

type RuleStorage interface {
	Store(ctx context.Context, spec S) error
	Lookup(ctx context.Context, id string) (S, error)
	Delete(ctx context.Context, id string) error
	ForEach(ctx context.Context, iter func(S) bool) bool
}

type StateStorage interface {
}
