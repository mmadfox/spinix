package spinix

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/tidwall/geojson"

	"github.com/tidwall/geojson/geometry"
)

func TestEngineAddRule(t *testing.T) {
	poly1 := polyFromString(`
-72.2863612, 42.9269489
-72.2864041, 42.9256605
-72.2844090, 42.9256134
-72.2843018, 42.9271532
-72.2863397, 42.9269646
-72.2863612, 42.9269489
`)
	poly2 := polyFromString(`
-72.2809119, 42.9210564
-72.2786809, 42.9195165
-72.2774581, 42.9205536
-72.2808905, 42.9210721
-72.2809119, 42.9210564
`)

	engine := New()
	ctx := context.Background()

	if err := engine.AddObject(ctx, "poly1", poly1); err != nil {
		t.Fatal(err)
	}
	if err := engine.AddObject(ctx, "poly2", poly2); err != nil {
		t.Fatal(err)
	}

	_, err := engine.AddRule(
		ctx,
		"some name",
		"id",
		`device intersects polygon(@poly1) or device intersects polygon(@poly2)`,
		42.9284992,
		-72.2775902,
		100,
	)
	if err != nil {
		t.Fatal(err)
	}

	events, err := engine.Detect(ctx, &Device{Latitude: 42.9262625, Longitude: -72.2848860})
	if err != nil {
		t.Fatal(err)
	}
	_ = events
}

func TestEngineDetect(t *testing.T) {
	testCases := []struct {
		name     string
		spec     string
		lat, lon float64
		meters   float64
		match    []Match
	}{
		{
			name:   "rule-1",
			spec:   `device intersects polygon(@poly1)`,
			lat:    42.9284835,
			lon:    -72.2775688,
			meters: 1000,
		},
	}

	engine := New()
	ctx := context.Background()
	owner := "owner"
	for _, tc := range testCases {
		rule, err := NewRule(tc.name, owner, tc.spec, tc.lat, tc.lon, tc.meters)
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.Rules().Insert(ctx, rule); err != nil {
			t.Fatal(err)
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
