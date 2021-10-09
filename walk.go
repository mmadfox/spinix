package spinix

type Visitor interface {
	Visit(Expr) Visitor
}

func WalkFunc(node Expr, fn func(Expr)) {
	Walk(walkFuncVisitor(fn), node)
}

type walkFuncVisitor func(Expr)

func (fn walkFuncVisitor) Visit(n Expr) Visitor { fn(n); return fn }

func Walk(v Visitor, node Expr) {
	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *ParenExpr:
		Walk(v, n.Expr)
	}
}
