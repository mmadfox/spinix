package spinix

import (
	"context"
	"testing"

	"github.com/rs/xid"

	"github.com/mmcloughlin/spherand"
)

func TestDevicesNearby(t *testing.T) {
	ctx := context.Background()
	device := NewMemoryDevices()
	if _, err := device.InsertOrReplace(ctx, &Device{
		ID:        xid.New(),
		Latitude:  42.9312947,
		Longitude: -72.2845321,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := device.InsertOrReplace(ctx, &Device{
		ID:        xid.New(),
		Latitude:  42.9316521,
		Longitude: -72.2841567,
	}); err != nil {
		t.Fatal(err)
	}
	var found int
	if err := device.Near(ctx, 42.9316717, -72.2846072, 1000000000,
		func(ctx context.Context, d *Device) error {
			found++
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	if have, want := found, 2; have != want {
		t.Fatalf("have %d, want %d devices", have, want)
	}
}

func BenchmarkDevicesNearby(b *testing.B) {
	ctx := context.Background()
	device := NewMemoryDevices()
	max := 100000
	coords := make([][2]float64, max)
	for i := 0; i < max; i++ {
		lat, lon := spherand.Geographical()
		if _, err := device.InsertOrReplace(ctx, &Device{
			ID:        xid.New(),
			Latitude:  lat,
			Longitude: lon,
		}); err != nil {
			b.Fatal(err)
		}
		coords[i] = [2]float64{lat, lon}
	}
	b.ResetTimer()
	n := 0
	for i := 0; i < b.N; i++ {
		if n > max-1 {
			n = 0
		}
		cord := coords[n]
		n++
		if err := device.Near(ctx, cord[0], cord[1], 10000,
			func(ctx context.Context, d *Device) error {
				return nil
			}); err != nil {
			b.Fatal(err)
		}
	}
}
