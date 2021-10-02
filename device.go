package georule

import "time"

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
