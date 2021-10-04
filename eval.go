package georule

import (
	"fmt"
	"math"
)

var (
	falseExpr = &BooleanLit{}
	epsilon   = 1e-6
)

func eval(expr Expr, device *Device, state *State) (Expr, error) {
	if expr == nil || device == nil || state == nil {
		return falseExpr, nil
	}
	var (
		err    error
		lv, rv Expr
	)

	switch n := expr.(type) {
	case *ParenExpr:
		return eval(n.Expr, device, state)
	case *BinaryExpr:
		lv, err = eval(n.LHS, device, state)
		if err != nil {
			return falseExpr, err
		}
		rv, err = eval(n.RHS, device, state)
		if err != nil {
			return falseExpr, err
		}
		return applyOperator(n.Op, lv, rv)
	case *VarLit:
		switch n.Value {
		case VAR_SPEED:
			return &FloatLit{Value: device.Speed}, nil
		case VAR_BATTERY:
			return &FloatLit{Value: device.BatteryCharge}, nil
		case VAR_TEMPERATURE:
			return &FloatLit{Value: device.Temperature}, nil
		case VAR_HUMIDITY:
			return &FloatLit{Value: device.Humidity}, nil
		case VAR_LUMONOSITY:
			return &FloatLit{Value: device.Luminosity}, nil
		case VAR_PRESSURE:
			return &FloatLit{Value: device.Pressure}, nil
		case VAR_FUELLEVEL:
			return &FloatLit{Value: device.FuelLevel}, nil
		case VAR_MODEL:
			return &StringLit{Value: device.Model}, nil
		case VAR_BRAND:
			return &StringLit{Value: device.Brand}, nil
		case VAR_OWNER:
			return &StringLit{Value: device.Owner}, nil
		case VAR_EMEI:
			return &StringLit{Value: device.IMEI}, nil
		case VAR_STATUS:
			return &IntLit{Value: device.Status}, nil
		}
	case *CallExpr:
	}
	return expr, nil
}

func applyOperator(op Token, l, r Expr) (*BooleanLit, error) {
	switch op {
	case AND:
		return applyAND(l, r) // AND
	case OR:
		return applyOR(l, r) // OR
	case GEQ:
		return applyGEQ(l, r) // >=
	case GTR:
		return applyGTR(l, r) // >
	case LEQ:
		return applyLEQ(l, r) // <=
	case LSS:
		return applyLSS(l, r) // <
	case NEQ:
		return applyNEQ(l, r) // !=
	case EQL:
		return applyEQL(l, r) // ==
	}
	return falseExpr, fmt.Errorf("georule: unsupported operator: %s", op)
}

// AND
func applyAND(l, r Expr) (*BooleanLit, error) {
	var (
		a, b bool
		err  error
	)
	a, err = booleanVal(l)
	if err != nil {
		return nil, err
	}
	b, err = booleanVal(r)
	if err != nil {
		return nil, err
	}
	return &BooleanLit{Value: a && b}, nil
}

// OR
func applyOR(l, r Expr) (*BooleanLit, error) {
	var (
		a, b bool
		err  error
	)
	a, err = booleanVal(l)
	if err != nil {
		return nil, err
	}
	b, err = booleanVal(r)
	if err != nil {
		return nil, err
	}
	return &BooleanLit{Value: a || b}, nil
}

// >=
func applyGEQ(l, r Expr) (*BooleanLit, error) {
	var (
		a, b float64
		err  error
	)
	a, err = numberVal(l)
	if err != nil {
		return nil, err
	}
	b, err = numberVal(r)
	if err != nil {
		return nil, err
	}
	return &BooleanLit{Value: (a > b) || float64Equal(a, b)}, nil
}

// >
func applyGTR(l, r Expr) (*BooleanLit, error) {
	var (
		a, b float64
		err  error
	)
	a, err = numberVal(l)
	if err != nil {
		return nil, err
	}
	b, err = numberVal(r)
	if err != nil {
		return nil, err
	}
	return &BooleanLit{Value: a > b}, nil
}

// <=
func applyLEQ(l, r Expr) (*BooleanLit, error) {
	var (
		a, b float64
		err  error
	)
	a, err = numberVal(l)
	if err != nil {
		return falseExpr, err
	}
	b, err = numberVal(r)
	if err != nil {
		return falseExpr, err
	}
	return &BooleanLit{Value: (a < b) || float64Equal(a, b)}, nil
}

// <
func applyLSS(l, r Expr) (*BooleanLit, error) {
	var (
		a, b float64
		err  error
	)
	a, err = numberVal(l)
	if err != nil {
		return falseExpr, err
	}
	b, err = numberVal(r)
	if err != nil {
		return falseExpr, err
	}
	return &BooleanLit{Value: a < b}, nil
}

// !=
func applyNEQ(l, r Expr) (*BooleanLit, error) {
	v, err := applyEQL(l, r)
	if err != nil {
		return nil, err
	}
	v.Value = !v.Value
	return v, nil
}

// ==
func applyEQL(l, r Expr) (*BooleanLit, error) {
	var (
		as, bs string
		an, bn float64
		ab, bb bool
		err    error
	)

	// strings
	as, err = stringVal(l)
	if err == nil {
		bs, err = stringVal(r)
		if err != nil {
			return falseExpr, fmt.Errorf("georule: cannot compare string with non-string")
		}
		return &BooleanLit{Value: as == bs}, nil
	}

	// numbers
	an, err = numberVal(l)
	if err == nil {
		bn, err = numberVal(r)
		if err != nil {
			return falseExpr, fmt.Errorf("georule: cannot compare number with non-number")
		}
		return &BooleanLit{Value: float64Equal(an, bn)}, nil
	}

	// boolean
	ab, err = booleanVal(l)
	if err == nil {
		bb, err = booleanVal(r)
		if err != nil {
			return falseExpr, fmt.Errorf("georule: cannot compare boolean with non-boolean")
		}
		return &BooleanLit{Value: ab == bb}, nil
	}

	return falseExpr, nil
}

func booleanVal(e Expr) (bool, error) {
	switch n := e.(type) {
	case *BooleanLit:
		return n.Value, nil
	default:
		return false, fmt.Errorf("georule: literal is not a boolean: %v", n)
	}
}

func numberVal(e Expr) (float64, error) {
	switch n := e.(type) {
	case *FloatLit:
		return n.Value, nil
	case *IntLit:
		return float64(n.Value), nil
	default:
		return 0, fmt.Errorf("georule: literal is not a number: %v", n)
	}
}

func stringVal(e Expr) (string, error) {
	switch n := e.(type) {
	case *StringLit:
		return n.Value, nil
	default:
		return "", fmt.Errorf("georule: literal is not a string: %v", n)
	}
}

func float64Equal(a float64, b float64) bool {
	absA := math.Abs(a)
	absB := math.Abs(b)
	diff := math.Abs(a - b)
	zero := float64(0)
	if a == b {
		return true
	}
	if diff > epsilon {
		return false
	}
	if a == zero || b == zero {
		return diff < epsilon*math.SmallestNonzeroFloat32
	}
	return diff/math.Min(absA+absB, math.MaxFloat64) < epsilon
}
