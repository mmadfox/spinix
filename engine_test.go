package spinix

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/tidwall/geojson"

	"github.com/tidwall/geojson/geometry"
)

func TestEngineDetectOneTimes(t *testing.T) {
	poly1 := polyFromString(`
-72.2368648, 42.3367342
-72.2353846, 42.3363298
-72.2350414, 42.3368453
-72.2367468, 42.3372973
-72.2368970, 42.3367660
-72.2368648, 42.3367342
`)
	devicePath := []geometry.Point{
		{X: 42.3372894, Y: -72.2353417},
		{X: 42.3367501, Y: -72.2359424},
		{X: 42.3362267, Y: -72.2362534},
	}

	ctx := context.Background()
	engine := New(
		WithDetectBefore(
			func(device *Device, rule *Rule) {
				t.Log("beforeDetect", device.IMEI, rule.Specification())
			}),
		WithDetectAfter(
			func(device *Device, rule *Rule, match bool, events []Event) {
				t.Log("afterDetect", device.IMEI, match, len(events))
			}),
	)

	_ = engine.Objects().Add(ctx, "poly", poly1)

	_, _ = engine.AddRule(ctx, `device intersects polygon(@poly) { :center 42.3351401 -72.236779 :radius 5km }`)

	var match int
	for _, p := range devicePath {
		device := &Device{IMEI: "test", Latitude: p.X, Longitude: p.Y}
		events, err := engine.Detect(ctx, device)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) > 0 {
			match++
		}
	}
	if match != 1 {
		t.Fatalf("have %d, want 1", match)
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
