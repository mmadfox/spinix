package spinix

import (
	pb "github.com/mmadfox/spinix/proto"
)

func ToRule(rule *Rule) *pb.Rule {
	p := new(pb.Rule)
	p.RuleId = rule.ID()
	p.RegionSize = int64(rule.RegionSize().Value())
	p.Spec = rule.Specification()
	p.RegionIds = make([]string, len(rule.RegionIDs()))
	regionIDs := rule.RegionIDs()
	for i := 0; i < len(regionIDs); i++ {
		p.RegionIds[i] = regionIDs[i].String()
	}
	return p
}

func FromRule(rule *pb.Rule) (*Rule, error) {
	regions := make([]RegionID, len(rule.RegionIds))
	for i := 0; i < len(regions); i++ {
		id, err := RegionIDFromString(rule.RegionIds[i])
		if err != nil {
			return nil, err
		}
		regions[i] = id
	}
	regionSize := RegionSize(rule.RegionSize)
	if err := regionSize.Validate(); err != nil {
		return nil, err
	}
	return RuleFromSpec(rule.RuleId, regions, regionSize, rule.Spec)
}

func ToDevice(device *Device) *pb.Device {
	p := new(pb.Device)
	p.Imei = device.IMEI
	p.Owner = device.Owner
	p.Brand = device.Brand
	p.Model = device.Model
	p.Latitude = device.Latitude
	p.Longitude = device.Longitude
	p.Altitude = device.Altitude
	p.Speed = device.Speed
	p.DateTime = device.DateTime
	p.Status = int64(device.Status)
	p.BatteryCharge = device.BatteryCharge
	p.Temperature = device.Temperature
	p.Humidity = device.Humidity
	p.Luminosity = device.Luminosity
	p.Pressure = device.Pressure
	p.FuelLevel = device.FuelLevel
	return p
}

func FromDevice(p *pb.Device) *Device {
	device := new(Device)
	device.IMEI = p.Imei
	device.Owner = p.Owner
	device.Brand = p.Brand
	device.Model = p.Model
	device.Latitude = p.Latitude
	device.Longitude = p.Longitude
	device.Altitude = p.Altitude
	device.Speed = p.Speed
	device.DateTime = p.DateTime
	device.Status = int(p.Status)
	device.BatteryCharge = p.BatteryCharge
	device.Temperature = p.Temperature
	device.Humidity = p.Humidity
	device.Luminosity = p.Luminosity
	device.Pressure = p.Pressure
	device.FuelLevel = p.FuelLevel
	return device
}
