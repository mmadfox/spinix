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
	// An IdentLit nodes represents an identifier.
	IdentLit struct {
		Name string
		Pos  Pos
		Kind Token
	}

	// A BinaryExpr nodes represents a binary expression.
	BinaryExpr struct {
		LHS Expr  // left operand
		Op  Token // operator
		RHS Expr  // right operand
	}

	// A ParenExpr nodes represents a parenthesized expression.
	ParenExpr struct {
		Expr Expr // parenthesized expression
	}

	PropExpr struct {
		Expr Expr
		List []Expr
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
		Pos      Pos
	}

	ResetLit struct {
		Kind  Token
		Pos   Pos
		After time.Duration
	}

	// A ListLit represents a list of int or float or string type.
	ListLit struct {
		Items []Expr
		Pos   Pos
		Kind  Token
		Typ   Token
	}

	// A StringLit nodes represents a literal of string type.
	StringLit struct {
		Value string
		Pos   Pos
	}

	// An IntLit nodes represents a literal of int type.
	IntLit struct {
		Value int
		Pos   Pos
	}

	// A FloatLit nodes represents a literal of float type.
	FloatLit struct {
		Value float64
		Pos   Pos
	}

	// A TimeLit nodes represents a literal of time type.
	TimeLit struct {
		Hour   int
		Minute int
		Pos    Pos
	}

	// A VarLit represents a variable literal.
	VarLit struct {
		Value Token
		Pos   Pos
	}

	// A BooleanLit represents a boolean literal.
	BooleanLit struct {
		Value bool
		Pos   Pos
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

func (e *DeviceLit) steps() (steps int) {
	switch e.Kind {
	case RADIUS:
		steps = 12
	case BBOX:
		steps = 4
	}
	return
}

func (e *DeviceLit) meters() float64 {
	switch e.Unit {
	case DistanceMeters:
		return e.Value
	case DistanceKilometers:
		return e.Value * 1000
	default:
		return 0
	}
}

func (e *DeviceLit) hasRadius() bool {
	switch e.Kind {
	case RADIUS, BBOX:
	default:
		return false
	}
	switch e.Unit {
	case DistanceMeters, DistanceKilometers:
		if e.Value > 0 {
			return true
		}
	}
	return false
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
	var sb strings.Builder
	sb.WriteString(TRIGGER.String())
	sb.WriteString(" ")
	switch e.Repeat {
	case RepeatTimes:
		sb.WriteString(strconv.Itoa(e.Times))
		sb.WriteString(" ")
		sb.WriteString("times")
		sb.WriteString(" ")
		sb.WriteString("interval")
		sb.WriteString(" ")
		sb.WriteString(e.Interval.String())
	case RepeatEvery:
		sb.WriteString("every")
		sb.WriteString(" ")
		sb.WriteString(e.Value.String())
	case RepeatOnce:
		sb.WriteString("once")
	default:
		sb.WriteString("once")
	}
	return sb.String()
}

func (e *PropExpr) String() string {
	var sb strings.Builder
	sb.WriteString(e.Expr.String())
	sb.WriteString(" ")
	for i := 0; i < len(e.List); i++ {
		sb.WriteString(e.List[i].String())
		sb.WriteString(" ")
	}
	return sb.String()
}

func (e *ResetLit) String() string {
	return fmt.Sprintf("%s after %s", RESET, e.After)
}

func (_ *ParenExpr) expr()  {}
func (_ *BinaryExpr) expr() {}
func (_ *StringLit) expr()  {}
func (_ *IntLit) expr()     {}
func (_ *FloatLit) expr()   {}
func (_ *VarLit) expr()     {}
func (_ *BooleanLit) expr() {}
func (_ *DeviceLit) expr()  {}
func (_ *ObjectLit) expr()  {}
func (_ *IdentLit) expr()   {}
func (_ *ListLit) expr()    {}
func (_ *DevicesLit) expr() {}
func (_ *TimeLit) expr()    {}
func (_ *PropExpr) expr()   {}
func (_ *TriggerLit) expr() {}
func (_ *ResetLit) expr()   {}
