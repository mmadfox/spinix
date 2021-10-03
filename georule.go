package georule

import (
	"fmt"
	"time"
)

type Device struct {
	IMEI          string
	Owner         string
	Brand         string
	Model         string
	Latitude      float64
	Longitude     float64
	Altitude      float64
	Speed         float64
	DateTime      time.Time
	Status        int
	BatteryCharge float64
	Temperature   float64
	Humidity      float64
	Luminosity    float64
	Pressure      float64
	FuelLevel     float64
}

type Context struct {
	Device *Device
	State  *State
}

type State struct {
}

func Detect(spec S, ctx *Context) (bool, error) {
	if spec.IsEmpty() {
		return false, nil
	}
	res, err := eval(spec.Expr(), ctx)
	if err != nil {
		return false, err
	}
	switch typ := res.(type) {
	case *BooleanLit:
		return typ.Value, nil
	}
	return false, fmt.Errorf("georule: unexpected result %#v", res)
}
