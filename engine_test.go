package spinix

import (
	"context"
	"log"
	"strconv"
	"strings"
	"testing"

	"github.com/mmcloughlin/spherand"

	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geometry"
)

func TestEngineDetect(t *testing.T) {
	engine := New()
	ctx := context.Background()
	poly1 := pointsFromString(`
-72.2815945, 42.9273078
-72.2812189, 42.9253909
-72.2786969, 42.9252102
-72.2782140, 42.9246367
-72.2761428, 42.9262079
-72.2764969, 42.9271035
-72.2773447, 42.9280462
-72.2772481, 42.9286747
-72.2805535, 42.9286432
-72.2815945, 42.9273313
`)
	engine.Map().Add(ctx, "poly1", geojson.NewPolygon(geometry.NewPoly(poly1, nil, nil)))
	rule, err := NewRule(
		"rule1",
		"withinPoly(@poly1)", 42.9275356, -72.2790618, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.InsertRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	myDevice := &Device{
		Latitude:  42.92675,
		Longitude: -72.2807359,
	}
	events, err := engine.Detect(ctx, myDevice)
	if err != nil {
		log.Fatal(err)
	}
	if len(events) == 0 {
		log.Fatalf("got 0, but expected 1 event")
	}
}

func BenchmarkEngineDetect(b *testing.B) {
	engine := New()
	ctx := context.Background()
	poly1 := pointsFromString(`
-72.2815945, 42.9273078
-72.2812189, 42.9253909
-72.2786969, 42.9252102
-72.2782140, 42.9246367
-72.2761428, 42.9262079
-72.2764969, 42.9271035
-72.2773447, 42.9280462
-72.2772481, 42.9286747
-72.2805535, 42.9286432
-72.2815945, 42.9273313
`)
	engine.Map().Add(ctx, "poly1", geojson.NewPolygon(geometry.NewPoly(poly1, nil, nil)))
	rule, err := NewRule(
		"rule1",
		"withinPoly(@poly1)", 42.9275356, -72.2790618, 5000)
	if err != nil {
		b.Fatal(err)
	}
	if err := engine.InsertRule(ctx, rule); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		lat, lon := spherand.Geographical()
		myDevice := &Device{
			Latitude:  lat,
			Longitude: lon,
		}
		b.StartTimer()
		_, err := engine.Detect(ctx, myDevice)
		if err != nil {
			log.Fatal(err)
		}
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
