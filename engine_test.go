package spinix

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/tidwall/geojson"

	"github.com/tidwall/geojson/geometry"
)

func TestInvokeSpecWithNearbyPolygon(t *testing.T) {
	engine := New()
	device := &Device{
		Latitude:  42.9236482,
		Longitude: -72.2793631,
	}
	ctx := context.Background()
	poly1 := polyFromString(`
-72.2801948, 42.9242649
-72.2781879, 42.9241588
-72.2781611, 42.9235500
-72.2784401, 42.9228154
-72.2797280, 42.9227368
-72.2803397, 42.9228782
-72.2806133, 42.9232278
-72.2805328, 42.9237542
-72.2801894, 42.9242688
`)
	poly2 := polyFromString(`
-72.2800882, 42.9299351
-72.2798950, 42.9290200
-72.2788326, 42.9290671
-72.2786233, 42.9294442
-72.2787521, 42.9298527
-72.2790848, 42.9299901
-72.2800989, 42.9299391
-72.2800882, 42.9299351
`)
	if err := engine.Map().Add(ctx, "poly1", poly1); err != nil {
		t.Fatal(err)
	}
	if err := engine.Map().Add(ctx, "poly2", poly2); err != nil {
		t.Fatal(err)
	}
	spec, err := ParseSpec(`device(@) nearby polygon(@poly1, @poly2) on distance 400`)
	if err != nil {
		t.Fatal(err)
	}
	expr, err := engine.InvokeSpec(ctx, spec, device)
	if err != nil {
		t.Fatal(err)
	}
	assertSpec(t, expr, spec.String())
}

func TestInvokeSpecWithNearbyDevices(t *testing.T) {
	engine := New()
	ctx := context.Background()
	devices := []*Device{
		{
			IMEI:      "device1",
			Latitude:  42.9294049,
			Longitude: -72.2791384,
		},
		{
			IMEI:      "device2",
			Latitude:  42.929291,
			Longitude: -72.2790794,
		},
		{
			IMEI:      "device3",
			Latitude:  42.9290475,
			Longitude: -72.2794335,
		},
	}
	for _, device := range devices {
		if err := engine.Devices().InsertOrReplace(ctx, device); err != nil {
			t.Fatal(err)
		}
	}

	spec, err := ParseSpec(`device(@) nearby device(device1, device2) on distance 400`)
	if err != nil {
		t.Fatal(err)
	}
	expr, err := engine.InvokeSpec(ctx, spec, devices[0])
	if err != nil {
		t.Fatal(err)
	}
	assertSpec(t, expr, spec.String())

	spec, err = ParseSpec(`device(device1, device2) nearby device(device3) on distance 1400`)
	if err != nil {
		t.Fatal(err)
	}
	expr, err = engine.InvokeSpec(ctx, spec, devices[0])
	if err != nil {
		t.Fatal(err)
	}
	assertSpec(t, expr, spec.String())
}

func BenchmarkInvokeSpecWithNearbyPolygon(b *testing.B) {
	engine := New()
	device := &Device{
		Latitude:  42.9236482,
		Longitude: -72.2793631,
	}
	ctx := context.Background()
	poly1 := polyFromString(`
-72.2801948, 42.9242649
-72.2781879, 42.9241588
-72.2781611, 42.9235500
-72.2784401, 42.9228154
-72.2797280, 42.9227368
-72.2803397, 42.9228782
-72.2806133, 42.9232278
-72.2805328, 42.9237542
-72.2801894, 42.9242688
`)
	poly2 := polyFromString(`
-72.2800882, 42.9299351
-72.2798950, 42.9290200
-72.2788326, 42.9290671
-72.2786233, 42.9294442
-72.2787521, 42.9298527
-72.2790848, 42.9299901
-72.2800989, 42.9299391
-72.2800882, 42.9299351
`)
	if err := engine.Map().Add(ctx, "poly1", poly1); err != nil {
		b.Fatal(err)
	}
	if err := engine.Map().Add(ctx, "poly2", poly2); err != nil {
		b.Fatal(err)
	}
	spec, err := ParseSpec(`device(@) nearby polygon(@poly1, @poly2) on distance 400`)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := engine.InvokeSpec(ctx, spec, device)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInvokeSpecWithNearbyDevices(b *testing.B) {
	engine := New()
	ctx := context.Background()
	devices := []*Device{
		{
			IMEI:      "device1",
			Latitude:  42.9294049,
			Longitude: -72.2791384,
		},
		{
			IMEI:      "device2",
			Latitude:  42.929291,
			Longitude: -72.2790794,
		},
		{
			IMEI:      "device3",
			Latitude:  42.9290475,
			Longitude: -72.2794335,
		},
	}
	for _, device := range devices {
		if err := engine.Devices().InsertOrReplace(ctx, device); err != nil {
			b.Fatal(err)
		}
	}

	spec, err := ParseSpec(`device(device1, device2) nearby device(device3) on distance 1400`)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.InvokeSpec(ctx, spec, devices[0])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func assertSpec(t *testing.T, expr Expr, spec string) {
	switch typ := expr.(type) {
	case *BooleanLit:
		if !typ.Value {
			t.Fatalf("engine.InvokeSpec(%s) => false, want true", spec)
		}
	default:
		t.Fatalf("engine.InvokeSpec(%s) returned not boolean literal", spec)
	}
}

func pointsFromString(s string) []geometry.Point {
	lines := strings.Split(s, "\n")
	res := make([]geometry.Point, 0)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		points := strings.Split(line, ",")
		lats := strings.Trim(points[1], " ")
		lons := strings.Trim(points[0], " ")
		if len(lats) == 0 && len(lons) == 0 {
			continue
		}
		lat, err := strconv.ParseFloat(lats, 10)
		if err != nil {
			panic(err)
		}
		lon, err := strconv.ParseFloat(lons, 10)
		if err != nil {
			panic(err)
		}
		res = append(res, geometry.Point{
			X: lat,
			Y: lon,
		})
	}
	return res
}

func polyFromString(s string) *geojson.Polygon {
	res := pointsFromString(s)
	return geojson.NewPolygon(geometry.NewPoly(res, nil, nil))
}
