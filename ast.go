package georule

import (
	"fmt"
	"strings"
)

type Expr interface {
	String() string

	expr()
}

type (
	// An Ident expr represents an identifier.
	Ident struct {
		Name string
	}

	// A UnaryExpr expr represents a unary expression.
	UnaryExpr struct {
		Op Token // operator
		X  Expr  // operand
	}

	// A BinaryExpr expr represents a binary expression.
	BinaryExpr struct {
		LHS Expr  // left operand
		Op  Token // operator
		RHS Expr  // right operand
	}

	// A ParenExpr expr represents a parenthesized expression.
	ParenExpr struct {
		Expr Expr // parenthesized expression
	}

	// An IndexExpr expr represnts an expression followed by an index.
	IndexExpr struct {
		Expr  Expr // expression
		Index Expr // index expression
	}

	// A CallExpr expr represents an expression followed by an argument list.
	CallExpr struct {
		Fun  Token  // keyword
		Args []Expr // function arguments; or nil
	}

	// A StringLit expr represents a literal of string type.
	StringLit struct {
		Value string
	}

	// A NumberLit expr represents a literal of int type.
	IntLit struct {
		Value int
	}

	// A FloatLit expr represents a literal of float type.
	FloatLit struct {
		Value float64
	}
)

func (e *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", e.Expr.String())
}

func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.LHS.String(), e.Op, e.RHS.String())
}

func (e *CallExpr) String() string {
	var sb strings.Builder
	li := len(e.Args) - 1
	sb.WriteString(LPAREN.String())
	for i, arg := range e.Args {
		sb.WriteString(arg.String())
		if i != li {
			sb.WriteString(COMMA.String())
		}
	}
	sb.WriteString(RPAREN.String())
	return fmt.Sprintf("%s%s", e.Fun, sb.String())
}

func (e *StringLit) String() string {
	return fmt.Sprintf("%s", e.Value)
}

func (e *IntLit) String() string {
	return fmt.Sprintf("%d", e.Value)
}

func (e *FloatLit) String() string {
	return fmt.Sprintf("%.2f", e.Value)
}

func (_ *ParenExpr) expr()  {}
func (_ *BinaryExpr) expr() {}
func (_ *CallExpr) expr()   {}
func (_ *StringLit) expr()  {}
func (_ *IntLit) expr()     {}
func (_ *FloatLit) expr()   {}
