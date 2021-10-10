package spinix

import (
	"context"
	"fmt"
	"math"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

var (
	falseExpr = &BooleanLit{Value: false}
	trueExpr  = &BooleanLit{Value: true}
	epsilon   = 1e-6
)

func (e *Engine) applyOperator(
	ctx context.Context,
	op Token,
	l, r Expr,
	prevState, currentState *Device,
) (Expr, error) {
	switch op {
	case AND:
		return e.applyAND(l, r) // AND
	case OR:
		return e.applyOR(l, r) // OR
	case GEQ:
		return e.applyGEQ(l, r) // >=
	case GTR:
		return e.applyGTR(l, r) // >
	case LEQ:
		return e.applyLEQ(l, r) // <=
	case LSS:
		return e.applyLSS(l, r) // <
	case NEQ:
		return e.applyNEQ(l, r) // !=
	case EQL:
		return e.applyEQL(l, r) // ==
	case ONDISTANCE:
		return e.applyONDISTANCE(ctx, l, r) // on distance
	case NEARBY:
		return e.applyNEARBY(ctx, l, r, prevState, currentState) // nearby
	}
	return falseExpr, fmt.Errorf("georule/eval: unsupported operator: %s", op)
}

// AND
func (e *Engine) applyAND(l, r Expr) (*BooleanLit, error) {
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
func (e *Engine) applyOR(l, r Expr) (*BooleanLit, error) {
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
func (e *Engine) applyGEQ(l, r Expr) (*BooleanLit, error) {
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
func (e *Engine) applyGTR(l, r Expr) (*BooleanLit, error) {
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
func (e *Engine) applyLEQ(l, r Expr) (*BooleanLit, error) {
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
func (e *Engine) applyLSS(l, r Expr) (*BooleanLit, error) {
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
func (e *Engine) applyNEQ(l, r Expr) (*BooleanLit, error) {
	v, err := e.applyEQL(l, r)
	if err != nil {
		return nil, err
	}
	v.Value = !v.Value
	return v, nil
}

// ==
func (e *Engine) applyEQL(l, r Expr) (*BooleanLit, error) {
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
			return falseExpr, fmt.Errorf("georule/eval: cannot compare string with non-string")
		}
		return &BooleanLit{Value: as == bs}, nil
	}

	// numbers
	an, err = numberVal(l)
	if err == nil {
		bn, err = numberVal(r)
		if err != nil {
			return falseExpr, fmt.Errorf("georule/eval: cannot compare number with non-number")
		}
		return &BooleanLit{Value: float64Equal(an, bn)}, nil
	}

	// boolean
	ab, err = booleanVal(l)
	if err == nil {
		bb, err = booleanVal(r)
		if err != nil {
			return falseExpr, fmt.Errorf("georule/eval: cannot compare boolean with non-boolean")
		}
		return &BooleanLit{Value: ab == bb}, nil
	}

	return falseExpr, nil
}

func (e *Engine) applyONDISTANCE(ctx context.Context, l, r Expr) (Expr, error) {
	var (
		onDistanceObject *ObjectOnDistanceLit
		onDistanceDevice *DeviceOnDistanceLit
	)

	// LHS
	switch typ := l.(type) {
	case *CallExpr:
		switch typ.Fun {
		default:
			return falseExpr, nil
		case FUN_DEVICE:
			onDistanceDevice = &DeviceOnDistanceLit{
				DeviceIDs: make(map[string]struct{}),
			}
			for _, arg := range typ.Args {
				onDistanceDevice.DeviceIDs[arg.String()] = struct{}{}
			}
		case FUN_LINE, FUN_MULTI_LINE, FUN_CIRCLE, FUN_POINT, FUN_MULTI_POINT,
			FUN_POLY, FUN_MULTI_POLY, FUN_FUT_COLLECTION, FUN_GEOM_COLLECTION, FUN_RECT:
			onDistanceObject = &ObjectOnDistanceLit{
				Objects: make([]Object, len(typ.Args)),
			}
			for i, objectID := range typ.Args {
				id := objectID.String()
				object, err := e.objects.Lookup(ctx, id)
				if err != nil {
					return falseExpr, nil
				}
				onDistanceObject.Objects[i] = Object{
					ID:   id,
					Data: object,
				}
			}
		}
	default:
		return nil, fmt.Errorf("georule/eval: literal on distance is not a geo object or device: %v", l)
	}

	// RHS
	meters, err := numberVal(r)
	if err != nil {
		return falseExpr, err
	}
	if meters <= 0 {
		return falseExpr, nil
	}

	if onDistanceDevice != nil {
		onDistanceDevice.Meters = meters
		return onDistanceDevice, nil
	}

	if onDistanceObject != nil {
		onDistanceObject.Meters = meters
		return onDistanceObject, nil
	}

	return falseExpr, nil
}

func (e *Engine) applyNEARBY(ctx context.Context, l, r Expr, _, currentState *Device) (*BooleanLit, error) {
	var deviceCoords [][2]float64

	// LHS
	switch typ := l.(type) {
	case *CallExpr:
		switch typ.Fun {
		default:
			deviceCoords = [][2]float64{{currentState.Latitude, currentState.Longitude}}
		case FUN_DEVICE:
			deviceCoords = make([][2]float64, 0, 2)
			if typ.UseCtx {
				deviceCoords = append(deviceCoords, [2]float64{currentState.Latitude, currentState.Longitude})
			}
			for _, deviceID := range typ.Args {
				device, err := e.devices.Lookup(ctx, deviceID.String())
				if err != nil {
					continue
				}
				deviceCoords = append(deviceCoords, [2]float64{device.Latitude, device.Longitude})
			}
		}
	}

	// RHS
	switch typ := r.(type) {
	default:
		return falseExpr, nil
	case *BooleanLit:
		return typ, nil
	case *DeviceOnDistanceLit:
		var counter int
		for _, deviceCoord := range deviceCoords {
			if err := e.devices.Nearby(ctx, deviceCoord[0], deviceCoord[1], typ.Meters,
				func(ctx context.Context, d *Device) error {
					_, found := typ.DeviceIDs[d.IMEI]
					if found {
						counter++
					}
					return nil
				}); err != nil {
				// TODO: add logger
				continue
			}
		}
		return &BooleanLit{Value: counter > 0}, nil
	case *ObjectOnDistanceLit:
		var counter int
		for _, deviceCoord := range deviceCoords {
			point := geojson.NewPoint(geometry.Point{X: deviceCoord[0], Y: deviceCoord[1]})
			for _, object := range typ.Objects {
				if object.Data.Distance(point) <= typ.Meters {
					counter++
				}
			}
		}
		return &BooleanLit{Value: counter > 0}, nil
	}
}

func booleanVal(e Expr) (bool, error) {
	switch n := e.(type) {
	case *BooleanLit:
		return n.Value, nil
	default:
		return false, fmt.Errorf("georule/eval: literal is not a boolean: %v", n)
	}
}

func numberVal(e Expr) (float64, error) {
	switch n := e.(type) {
	case *FloatLit:
		return n.Value, nil
	case *IntLit:
		return float64(n.Value), nil
	default:
		return 0, fmt.Errorf("georule/eval: literal is not a number: %v", n)
	}
}

func stringVal(e Expr) (string, error) {
	switch n := e.(type) {
	case *StringLit:
		return n.Value, nil
	default:
		return "", fmt.Errorf("georule/eval: literal is not a string: %v", n)
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
