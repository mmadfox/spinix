package spinix

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/tidwall/geojson"

	"github.com/tidwall/geojson/geometry"
)

type testCase struct {
	spec     string
	route    []*Device
	imei     string
	populate func(e *Engine)
	match    []Event
	matchLen int
	err      bool
}

func TestEngineDetectIntersects(t *testing.T) {
	ctx := context.Background()
	testCases := []testCase{
		{
			imei: "qwe34q",
			spec: `device INTERSECTS objects(@id1) OR device INTERSECTS objects(@id2)
                { 
                   :center 42.9314328 -72.2812945 
                }`,
			route: []*Device{
				{Latitude: 42.9318155, Longitude: -72.2764766},
				{Latitude: 42.9317998, Longitude: -72.2771417},
				{Latitude: 42.9315013, Longitude: -72.2793513},
				{Latitude: 42.9310400, Longitude: -72.2829678},
				{Latitude: 42.9308672, Longitude: -72.2851988},
			},
			matchLen: 3,
			populate: func(e *Engine) {
				// lon lat
				o1 := polyFromString(`
-72.2857655, 42.9312970
-72.2856582, 42.9303544
-72.2822902, 42.9306686
-72.2824833, 42.9317841
-72.2857441, 42.9313285
-72.2857655, 42.9312970
`)
				// lon lat
				o2 := polyFromString(`
-72.2804024, 42.9320826
-72.2802737, 42.9308571
-72.2779998, 42.9311085
-72.2781928, 42.9323182
-72.2804239, 42.9320826
-72.2804024, 42.9320826
`)
				if err := e.Objects().Add(ctx, "id1", o1); err != nil {
					t.Fatal(err)
				}
				if err := e.Objects().Add(ctx, "id2", o2); err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, tc := range testCases {
		engine := New()

		// add geo objects on the map
		tc.populate(engine)

		// add new rule on the map
		if _, err := engine.AddRule(ctx, tc.spec); err != nil {
			t.Fatal(err)
		}

		matchedEvents := make([]Event, 0, 2)

		// walk the route
		for _, device := range tc.route {
			device.IMEI = tc.imei
			events, ok, err := engine.Detect(ctx, device)
			if err != nil {
				if tc.err {
					continue
				}
				t.Fatal(err)
			}
			if ok {
				matchedEvents = append(matchedEvents, events...)
			}
		}

		// asserts
		if have, want := len(matchedEvents), tc.matchLen; have != want {
			t.Fatalf("have %d, want %d matched events", have, want)
		}
	}
}

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
			func(device *Device, rule *Rule) bool {
				t.Log("beforeDetect", device.IMEI, rule.Specification())
				return true
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
		events, _, err := engine.Detect(ctx, device)
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

func collectionFromString(sets ...string) *geojson.GeometryCollection {
	objects := make([]geojson.Object, 0, len(sets))
	for _, set := range sets {
		res := pointsFromString(set)
		poly := geojson.NewPolygon(geometry.NewPoly(res, nil, nil))
		objects = append(objects, poly)
	}
	return geojson.NewGeometryCollection(objects)
}
