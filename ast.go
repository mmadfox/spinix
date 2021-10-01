package georule

import (
	"fmt"
	"strings"
)

type Node interface {
	node()
	String() string
}

type Expr interface {
	Node
	expr()
}

type (
	// An Ident node represents an identifier.
	Ident struct {
		NamePos Pos    // identifier position
		Name    string // identifier name
	}

	// A UnaryExpr node represents a unary expression.
	UnaryExpr struct {
		OpPos Pos   // position of Op
		Op    Token // operator
		X     Expr  // operand
	}

	// A BinaryExpr node represents a binary expression.
	BinaryExpr struct {
		LHS   Expr  // left operand
		OpPos Pos   // position of Op
		Op    Token // operator
		RHS   Expr  // right operand
	}

	// A ParenExpr node represents a parenthesized expression.
	ParenExpr struct {
		Lparen Pos  // position of "("
		Expr   Expr // parenthesized expression
		Rparen Pos  // position of ")"
	}

	// An IndexExpr node represnts an expression followed by an index.
	IndexExpr struct {
		Expr   Expr // expression
		Lbrack Pos  // position of "["
		Index  Expr // index expression
		Rbrack Pos  // position of "]"
	}

	// A CallExpr node represents an expression followed by an argument list.
	CallExpr struct {
		Fun    Token  // keyword
		Lparen Pos    // position of "("
		Args   []Expr // function arguments; or nil
		Rparen Pos    // position of ")"
	}

	// A BasicLit node represents a literal of basic type.
	BasicLit struct {
		ValuePos Pos    // literal position
		Kind     Token  // token.INT, token.FLOAT, token.STRING
		Value    string // literal string; e.g. 42, 0x7f, 3.14, 1e-9, 2.4i, 'a', '\x7f', "foo" or `\m\n\o`
	}
)

type ExprStmt struct {
	Expr Expr // expression
}

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

func (e *BasicLit) String() string {
	return fmt.Sprintf("%s%s", VAR, e.Value)
}

func (_ *ParenExpr) node()  {}
func (_ *BinaryExpr) node() {}
func (_ *CallExpr) node()   {}
func (_ *BasicLit) node()   {}

func (_ *ParenExpr) expr()  {}
func (_ *BinaryExpr) expr() {}
func (_ *CallExpr) expr()   {}
func (_ *BasicLit) expr()   {}
