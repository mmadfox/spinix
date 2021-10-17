package spinix

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/geojson"
)

type Expr interface {
	String() string

	expr()
}

type DistanceUnit int

const (
	DistanceUndefined  DistanceUnit = 0
	DistanceMeters     DistanceUnit = 1
	DistanceKilometers DistanceUnit = 2
)

func (u DistanceUnit) String() string {
	switch u {
	case DistanceMeters:
		return "m"
	case DistanceKilometers:
		return "km"
	default:
		return "#?"
	}
}

type RepeatMode int

const (
	RepeatOnce  RepeatMode = 1
	RepeatEvery RepeatMode = 2
	RepeatTimes RepeatMode = 3
)

func (rm RepeatMode) String() string {
	switch rm {
	case RepeatEvery:
		return "every"
	case RepeatOnce:
		return "once"
	case RepeatTimes:
		return "times"
	default:
		return "#?"
	}
}

type (
	// An IdentLit expr represents an identifier.
	IdentLit struct {
		Name string
		Pos  Pos
		Kind Token
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

	SpecExpr struct {
		Expr    Expr // expression
		Trigger Expr // index expression
	}

	DeviceLit struct {
		Unit  DistanceUnit
		Kind  Token
		Value float64
		Pos   Pos
	}

	DevicesLit struct {
		Unit  DistanceUnit
		Kind  Token
		Value float64
		Pos   Pos
		Ref   []string
	}

	ObjectLit struct {
		Kind   Token
		Ref    []string
		DurVal time.Duration
		DurTyp Token
		Pos    Pos
	}

	// A TriggerLit represents a repeat mode type.
	TriggerLit struct {
		Repeat   RepeatMode
		Interval time.Duration
		Value    time.Duration
		Times    int
	}

	// A ListLit represents a list of int or float or string type.
	ListLit struct {
		Items []Expr
		Pos   Pos
		Kind  Token
		Typ   Token
	}

	// A DistanceUnitLit represents a distance unit type.
	DistanceUnitLit struct {
		Value float64
		Op    Token
		Unit  DistanceUnit
	}

	// A StringLit expr represents a literal of string type.
	StringLit struct {
		Value string
	}

	// An IntLit expr represents a literal of int type.
	IntLit struct {
		Value int
	}

	// A FloatLit expr represents a literal of float type.
	FloatLit struct {
		Value float64
	}

	// A TimeLit expr represents a literal of time type.
	TimeLit struct {
		Hour   int
		Minute int
	}

	// A VarLit represents a variable literal.
	VarLit struct {
		Value Token
	}

	// A BooleanLit represents a boolean literal.
	BooleanLit struct {
		Value bool
	}
)

type Object struct {
	ID   string
	Data geojson.Object
}

func (e *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", e.Expr.String())
}

func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.LHS.String(), e.Op, e.RHS.String())
}

func (e *StringLit) String() string {
	return fmt.Sprintf("%s", e.Value)
}

func (e *IntLit) String() string {
	return fmt.Sprintf("%d", e.Value)
}

func (e FloatLit) String() string {
	return fmt.Sprintf("%.2f", e.Value)
}

func (e *VarLit) String() string {
	return fmt.Sprintf("{%s}", e.Value)
}

func (e *ListLit) String() string {
	var sb strings.Builder
	li := len(e.Items) - 1
	sb.WriteString("[")
	for i, expr := range e.Items {
		sb.WriteString(expr.String())
		if i != li {
			if e.Kind == RANGE {
				sb.WriteString(" .. ")
			} else {
				sb.WriteString(COMMA.String())
			}
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func (e *BooleanLit) String() string {
	if e.Value {
		return "true"
	} else {
		return "false"
	}
}

func (e *DistanceUnitLit) String() string {
	return fmt.Sprintf("%.1f%s", e.Value, e.Unit)
}

func (e *DeviceLit) String() string {
	var sb strings.Builder
	sb.WriteString("device")
	writeProps := func(name string) {
		sb.WriteString(" :")
		sb.WriteString(name)
		sb.WriteString(" ")
		sb.WriteString(fmt.Sprintf("%.1f", e.Value))
		sb.WriteString(e.Unit.String())
	}
	switch e.Kind {
	case BBOX:
		writeProps("bbox")
	case RADIUS:
		writeProps("radius")
	}
	return sb.String()
}

func (e *ObjectLit) String() string {
	var sb strings.Builder
	sb.WriteString(e.Kind.String())
	sb.WriteString("(")
	last := len(e.Ref) - 1
	for i, ref := range e.Ref {
		sb.WriteString("@" + ref)
		if i != last {
			sb.WriteString(",")
		}
	}
	writeProps := func(name string) {
		sb.WriteString(" :time ")
		sb.WriteString(name)
		sb.WriteString(" ")
		sb.WriteString(e.DurVal.String())
	}
	sb.WriteString(")")
	switch e.DurTyp {
	case DURATION:
		writeProps("duration")
	case AFTER:
		writeProps("after")
	}
	return sb.String()
}

func (e *IdentLit) String() string {
	return e.Kind.String()
}

func (e *TimeLit) String() string {
	var str string
	h := strconv.Itoa(e.Hour)
	m := strconv.Itoa(e.Minute)
	if e.Hour < 10 {
		str += "0" + h
	} else {
		str += h
	}
	str += ":"
	if e.Minute < 10 {
		str += "0" + m
	} else {
		str += m
	}
	return str
}

func (e *DevicesLit) String() string {
	var sb strings.Builder
	sb.WriteString("devices")
	sb.WriteString("(")
	last := len(e.Ref) - 1
	for i, ref := range e.Ref {
		sb.WriteString("@" + ref)
		if i != last {
			sb.WriteString(",")
		}
	}
	sb.WriteString(")")
	writeProps := func(name string) {
		sb.WriteString(" :")
		sb.WriteString(name)
		sb.WriteString(" ")
		sb.WriteString(fmt.Sprintf("%.1f", e.Value))
		sb.WriteString(e.Unit.String())
	}
	switch e.Kind {
	case BBOX:
		writeProps("bbox")
	case RADIUS:
		writeProps("radius")
	}
	return sb.String()
}

func (e *TriggerLit) String() string {
	switch e.Repeat {
	case RepeatTimes:
		return fmt.Sprintf("%d times internval %s", e.Times, e.Interval)
	case RepeatEvery:
		return fmt.Sprintf("every %s", e.Value)
	case RepeatOnce:
		return "once"
	default:
		return "once"
	}
}

func (e *SpecExpr) String() string {
	return e.Expr.String() + " :trigger " + e.Trigger.String()
}

func (_ *ParenExpr) expr()  {}
func (_ *BinaryExpr) expr() {}
func (_ *StringLit) expr()  {}
func (_ *IntLit) expr()     {}
func (_ *FloatLit) expr()   {}
func (_ *VarLit) expr()     {}
func (_ *BooleanLit) expr() {}

func (_ *DistanceUnitLit) expr() {} // deprecated

//
func (_ *DeviceLit) expr()  {}
func (_ *ObjectLit) expr()  {}
func (_ *IdentLit) expr()   {}
func (_ *ListLit) expr()    {}
func (_ *DevicesLit) expr() {}
func (_ *TimeLit) expr()    {}
func (_ *TriggerLit) expr() {}
func (_ *SpecExpr) expr()   {}
