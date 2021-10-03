package georule

import (
	"fmt"
)

type S struct {
	id   string
	name string
	expr Expr
}

func Spec(id string, name string, spec string) (S, error) {
	if len(spec) == 0 {
		return S{}, fmt.Errorf("georule: specification not defined")
	}
	expr, err := ParseString(spec)
	if err != nil {
		return S{}, err
	}
	return S{
		id:   id,
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

func (r S) ID() string {
	return r.id
}

func (r S) Expr() Expr {
	return r.expr
}

func (r S) IsEmpty() bool {
	return r.expr == nil
}
