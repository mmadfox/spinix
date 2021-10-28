package spinix

import (
	"context"
	"testing"

	"github.com/tidwall/geojson"
)

var polytest = polyFromString(`
-72.2808218, 42.9279834
-72.2805106, 42.9266950
-72.2789867, 42.9268207
-72.2792657, 42.9282269
-72.2808218, 42.9280226
-72.2808218, 42.9279834
`)

func TestRuntimeIntersects(t *testing.T) {
	testCases := []struct {
		name         string
		spec         string
		device       *Device
		otherDevices []*Device
		match        []Match
		object       geojson.Object
		refsCount    int
		rid          string
		err          bool
	}{
		// success
		{
			name:         "should be successful when the my device intersects the other devices",
			spec:         `devices(@my) nintersects devices(@) { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, NINTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9306625, Longitude: -72.2847043}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:         "should be successful when the my device intersects the other devices",
			spec:         `devices(@my) intersects devices(@) { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:         "should be successful when the my device intersects the other devices at a distance of 100 meters",
			spec:         `devices(@my) intersects devices(@) :radius 100m { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:         "should be successful when the my device intersects the other devices at a distance of 100 meters",
			spec:         `device intersects devices(@other) :radius 100m { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:         "should be successful when the my device intersects the other devices at a distance of 100 meters",
			spec:         `device :radius 100m intersects devices(@other)  { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:         "should be successful when the my device intersects the other devices at a distance of 100 meters",
			spec:         `device :radius 100m intersects devices(@other) :bbox 400m  { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:         "should be successful when the my device intersects the other devices with bounding box at a radius 100 meters",
			spec:         `devices(@my) intersects devices(@) :bbox 100m { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:         "should be successful when the my device intersects the other devices at a distance of 100 meters",
			spec:         `devices(@my) :radius 100m intersects devices(@) { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:         "should be successful when the current device intersects the other device at a distance of 100 meters",
			spec:         `devices(@other) :radius 100m intersects device { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:         "should be successful when the current device intersects the other device with bounding box at a radius 100 meters",
			spec:         `devices(@other) :bbox 100m intersects device { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule294",
		},
		{
			name:         "should be successful when the current device intersects the other device",
			spec:         `devices(@other) intersects device { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule294",
		},
		{
			name:      "should be successful when the current device intersects the polygon with @object id",
			spec:      `device intersects polygon(@object)  { :center 42.9284788 72.2776118 }`,
			device:    &Device{IMEI: "current", Latitude: 42.9275513, Longitude: -72.2799653},
			match:     []Match{match(DEVICE, POLY, INTERSECTS)},
			object:    polytest,
			refsCount: 1,
			rid:       "rule999",
		},
		{
			name:      "should be successful when the current device intersects the polygon with @object id",
			spec:      `polygon(@object) intersects device { :center 42.9284788 72.2776118 }`,
			device:    &Device{IMEI: "current", Latitude: 42.9275513, Longitude: -72.2799653},
			match:     []Match{match(DEVICE, POLY, INTERSECTS)},
			object:    polytest,
			refsCount: 1,
			rid:       "rule999",
		},
		{
			name:         "should be successful when the current device intersects the other device at a distance of 100 meters",
			spec:         `device :radius 100m intersects devices(@other) :radius 100m { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "current", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule34",
		},
		{
			name:         "should be successful when the my device intersects the other device at a distance of 100 meters",
			spec:         `devices(@my) :radius 100m intersects devices(@other) :radius 100m { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:         "should be successful when the my device intersects the devices with bounding box at a radius 100 meters",
			spec:         `devices(@my) :bbox 100m intersects devices(@) :bbox 100m { :center 42.9284788 72.2776118 }`,
			device:       &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:        []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{{IMEI: "other", Latitude: 42.9284788, Longitude: -72.2776118}},
			refsCount:    1,
			rid:          "rule21",
		},
		{
			name:   "should be successful when the my device intersects the all devices at a distance of 100 meters",
			spec:   `devices(@my) :radius 100m intersects devices(@) :radius 100m { :center 42.9284788 72.2776118 }`,
			device: &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:  []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{
				{IMEI: "other1", Latitude: 42.9284788, Longitude: -72.2776118},
				{IMEI: "other2", Latitude: 42.9284788, Longitude: -72.2776118},
				{IMEI: "other3", Latitude: 42.9284788, Longitude: -72.2776118},
			},
			refsCount: 3,
			rid:       "rule99",
		},
		{
			name:   "should be successful when the my device intersects the all devices at a distance of 100 meters",
			spec:   `devices(@) :radius 100m intersects devices(@my) :radius 100m { :center 42.9284788 72.2776118 }`,
			device: &Device{IMEI: "my", Latitude: 42.9284788, Longitude: -72.2776118},
			match:  []Match{match(DEVICE, DEVICES, INTERSECTS)},
			otherDevices: []*Device{
				{IMEI: "other1", Latitude: 42.9284788, Longitude: -72.2776118},
				{IMEI: "other2", Latitude: 42.9284788, Longitude: -72.2776118},
				{IMEI: "other3", Latitude: 42.9284788, Longitude: -72.2776118},
			},
			refsCount: 3,
			rid:       "rule20",
		},

		// fails
		{
			spec: `devices(@) :radius 100m intersects devices(@) :radius 100m { :center 42.9284788 72.2776118 }`,
			err:  true,
		},
		{
			spec: `device  intersects device { :center 42.9284788 72.2776118 }`,
			err:  true,
		},
		{
			name: "invalid device specification => got device 100m, expected device :radius 100m",
			spec: `device 100m intersects polygon(@object)  { :center 42.9284788 72.2776118 }`,
			err:  true,
		},
		{
			name: "invalid device specification",
			spec: `device intersects temperature { :center 42.9284788 72.2776118 }`,
			err:  true,
		},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		refs := defaultRefs()

		for _, otherDevice := range tc.otherDevices {
			if _, err := refs.devices.InsertOrReplace(ctx, otherDevice); err != nil {
				t.Fatal(err)
			}
		}

		if tc.object != nil {
			if err := refs.objects.Add(ctx, "object", tc.object); err != nil {
				t.Fatal(err)
			}
		}

		spec, err := specFromString(tc.spec)
		if err != nil {
			if tc.err {
				continue
			} else {
				t.Fatalf("parseSpec(%s) => %v", tc.spec, err)
			}
		} else if tc.err {
			t.Fatalf("parseSpec(%s) => got nil, expected err", tc.spec)
		}
		matches, _, err := spec.evaluate(ctx, tc.rid, tc.device, refs)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := len(matches), len(tc.match); have != want {
			t.Fatalf("parseSpec(%s) => got %v, expected %v", tc.spec, have, want)
		}
		for i, m := range matches {
			if have, want := len(m.Right.Refs), tc.refsCount; have != want {
				t.Fatalf("parseSpec(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Ok, tc.match[i].Ok; have != want {
				t.Fatalf("parseSpec(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Left.Keyword, tc.match[i].Left.Keyword; have != want {
				t.Fatalf("parseSpec(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Right.Keyword, tc.match[i].Right.Keyword; have != want {
				t.Fatalf("parseSpec(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Operator, tc.match[i].Operator; have != want {
				t.Fatalf("parseSpec(%s) => got %v, expected %v", tc.spec, have, want)
			}
		}
	}
}

func match(left, right, op Token) Match {
	return Match{
		Ok:       true,
		Left:     Decl{Keyword: left},
		Right:    Decl{Keyword: right},
		Operator: op,
	}
}
