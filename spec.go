package georule

import (
	"fmt"

	"github.com/rs/xid"
)

type S struct {
	id   xid.ID
	name string
	expr Expr
}

func Spec(name string, spec string) (S, error) {
	if len(spec) == 0 {
		return S{}, fmt.Errorf("georule: specification not defined")
	}
	expr, err := ParseString(spec)
	if err != nil {
		return S{}, err
	}
	return S{
		id:   xid.New(),
		name: name,
		expr: expr,
	}, nil
}

func (r S) String() string {
	return r.expr.String()
}

func (r S) Name() string {
	return r.name
}

func (r S) ID() xid.ID {
	return r.id
}

func (r S) Expr() Expr {
	return r.expr
}

func (r S) IsEmpty() bool {
	return r.expr == nil
}
