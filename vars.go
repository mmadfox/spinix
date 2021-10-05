package georule

import (
	"context"
)

type Vars interface {
	Lookup(ctx context.Context, id string) (interface{}, error)
}
