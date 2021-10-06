package georule

import (
	"sync"
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

type State struct {
	imei          string
	owner         string
	brand         string
	model         string
	latitude      float64
	longitude     float64
	altitude      float64
	speed         float64
	dateTime      time.Time
	status        int
	batteryCharge float64
	temperature   float64
	humidity      float64
	luminosity    float64
	pressure      float64
	fuelLevel     float64
	mu            sync.RWMutex
	init          bool
}

func NewState(imei string) *State {
	return &State{
		imei: imei,
	}
}

func (s *State) IMEI() string {
	return s.imei
}

func (s *State) Update(device *Device) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.imei = device.IMEI
	s.owner = device.Owner
	s.brand = device.Brand
	s.model = device.Model
	s.latitude = device.Latitude
	s.longitude = device.Longitude
	s.altitude = device.Altitude
	s.speed = device.Speed
	s.dateTime = device.DateTime
	s.status = device.Status
	s.batteryCharge = device.BatteryCharge
	s.temperature = device.Temperature
	s.humidity = device.Humidity
	s.luminosity = device.Luminosity
	s.pressure = device.Pressure
	s.fuelLevel = device.FuelLevel
}
