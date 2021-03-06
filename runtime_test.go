package spinix

import (
	"context"
	"testing"

	"github.com/rs/xid"
)

type rTestCase struct {
	spec     []string
	target   *Device
	match    []Match
	populate func(refs reference)
	err      bool
}

func TestRuntimeInDevicesDevices(t *testing.T) {
	specs := []rTestCase{
		{ // @ <- all devices
			spec: []string{
				`device :radius 1km IN devices(@) :radius 100m`,
				`device :radius 1km IN devices(@)`,
				`device :bbox 1km IN devices(@)`,
				`devices(c5vj26evvhfjvfseauk0) :radius 2km IN devices(c5vj26evvhfjvfseauog)`,
			},
			target: makeDevice("c5vj26evvhfjvfseauk0", 42.9236468, -72.2793728),
			match:  []Match{match(DEVICE, DEVICES, IN)},
			populate: func(refs reference) {
				_, _ = refs.devices.InsertOrReplace(context.TODO(),
					makeDevice("c5vj26evvhfjvfseauog", 42.9229004, -72.2791582))
			},
		},
	}

	assertRuntimeTestCase(t, specs)
}

func TestRuntimeNotIntersectsDevicesDevices(t *testing.T) {
	specs := []rTestCase{
		{ // @ <- all devices
			spec: []string{
				`devices(c5vj26evvhfjvfseauk0) :radius 1km NINTERSECTS devices(@) :radius 1km`,
				`devices(c5vj26evvhfjvfseauk0) :bbox 1km NINTERSECTS devices(@) :bbox 1km`,
				`devices(c5vj26evvhfjvfseauk0) :radius 1km NINTERSECTS devices(c5vj26evvhfjvfseauog) :radius 500m`,
				`devices(c5vj26evvhfjvfseauk0) NINTERSECTS devices(c5vj26evvhfjvfseauog) :radius 500m`,
				`devices(c5vj26evvhfjvfseauk0) NINTERSECTS devices(c5vj26evvhfjvfseauog) :bbox 500m`,
				`devices(c5vj26evvhfjvfseauk0) :bbox 500m NINTERSECTS devices(c5vj26evvhfjvfseauog) :bbox 500m`,
				`devices(c5vj26evvhfjvfseauk0) :bbox 500m NINTERSECTS devices(c5vj26evvhfjvfseauog)`,
				`devices(c5vj26evvhfjvfseauk0) :bbox 500m NINTERSECTS devices(c5vj26evvhfjvfseauog)`,
			},
			target: makeDevice("c5vj26evvhfjvfseauk0", 42.9246289, -72.2876353),
			match:  []Match{match(DEVICE, DEVICES, NINTERSECTS)},
			populate: func(refs reference) {
				_, _ = refs.devices.InsertOrReplace(context.TODO(),
					makeDevice("c5vj26evvhfjvfseauog", 42.9152319, -72.2498989))
			},
		},
		{
			spec: []string{
				`devices(c5vj26evvhfjvfseauk0) NINTERSECTS devices(c5vj26evvhfjvfseauog)`,
				`devices(c5vj26evvhfjvfseauk0)  NINTERSECTS devices(@)`,
				`devices(c5vj26evvhfjvfseauk0) :radius 300m  NINTERSECTS devices(@)`,
				`devices(c5vj26evvhfjvfseauk0)  NINTERSECTS devices(@) :bbox 300m`,
				`devices(c5vj26evvhfjvfseauk0)  NINTERSECTS devices(@) :radius 300m`,
			},
			target: makeDevice("c5vj26evvhfjvfseauk0", 42.9246289, -72.2876353),
			match:  []Match{match(DEVICE, DEVICES, NINTERSECTS)},
			populate: func(refs reference) {
				_, _ = refs.devices.InsertOrReplace(context.TODO(),
					makeDevice("c5vj26evvhfjvfseauog", 42.9152319, -72.2498989))
			},
		},
	}

	assertRuntimeTestCase(t, specs)
}

func TestRuntimeIntersectsDevicesObjects(t *testing.T) {
	specs := []rTestCase{
		{ // @ <- all objects
			spec: []string{
				`devices(c5vj26evvhfjvfseauk0) INTERSECTS polygon(c5vj26evvhfjvfseaulg)`,
				`devices(c5vj26evvhfjvfseauk0) :radius 1km INTERSECTS polygon(@)`,
				`polygon(c5vj26evvhfjvfseaulg) INTERSECTS devices(c5vj26evvhfjvfseauk0)`,
			},
			target: makeDevice("c5vj26evvhfjvfseauk0", 42.9236075, -72.2792333),
			match:  []Match{match(DEVICE, POLY, INTERSECTS)},
			populate: func(refs reference) {
				obj := str2obj("c5vj26evvhfjvfseaulg", `
-72.2800060, 42.9238589
-72.2802743, 42.9231989
-72.2790616, 42.9232461
-72.2787397, 42.9239689
-72.2799953, 42.9238746
-72.2800060, 42.9238589
`)
				ctx := context.TODO()
				_ = refs.objects.Add(ctx, obj)
			},
		},
	}

	assertRuntimeTestCase(t, specs)
}

func TestRuntimeIntersectsDevicesDevices(t *testing.T) {
	specs := []rTestCase{
		{ // @ <- all devices
			spec: []string{
				`device :radius 1km INTERSECTS devices(@) :radius 1km`,
				`devices(@) :radius 1km INTERSECTS devices(c5vj26evvhfjvfseauk0) :radius 1km`,
				`devices(c5vj26evvhfjvfseauk0) :radius 1km INTERSECTS devices(@) :radius 1km`,
				`devices(c5vj26evvhfjvfseauk0) :bbox 1km INTERSECTS devices(@) :bbox 1km`,
				`devices(c5vj26evvhfjvfseauk0) :radius 1km INTERSECTS devices(c5vj26evvhfjvfseauog) :radius 500m`,
				`devices(c5vj26evvhfjvfseauk0) INTERSECTS devices(c5vj26evvhfjvfseauog) :radius 500m`,
				`devices(c5vj26evvhfjvfseauk0) INTERSECTS devices(c5vj26evvhfjvfseauog) :bbox 500m`,
				`devices(c5vj26evvhfjvfseauk0) :bbox 500m INTERSECTS devices(c5vj26evvhfjvfseauog) :bbox 500m`,
				`devices(c5vj26evvhfjvfseauk0) :bbox 500m INTERSECTS devices(c5vj26evvhfjvfseauog)`,
				`devices(c5vj26evvhfjvfseauk0) :bbox 500m INTERSECTS devices(c5vj26evvhfjvfseauog)`,
			},
			target: makeDevice("c5vj26evvhfjvfseauk0", 42.9214863, -72.2759164),
			match:  []Match{match(DEVICE, DEVICES, INTERSECTS)},
			populate: func(refs reference) {
				_, _ = refs.devices.InsertOrReplace(context.TODO(),
					makeDevice("c5vj26evvhfjvfseauog", 42.9236704, -72.2768608))
			},
		},
		{
			spec: []string{
				`devices(c5vj26evvhfjvfseauk0) INTERSECTS devices(c5vj26evvhfjvfseauog)`,
				`devices(c5vj26evvhfjvfseauk0)  INTERSECTS devices(@)`,
				`devices(c5vj26evvhfjvfseauk0) :radius 300m  INTERSECTS devices(@)`,
				`devices(c5vj26evvhfjvfseauk0)  INTERSECTS devices(@) :bbox 300m`,
				`devices(c5vj26evvhfjvfseauk0)  INTERSECTS devices(@) :radius 300m`,
			},
			target: makeDevice("c5vj26evvhfjvfseauk0", 42.9214863, -72.2759164),
			match:  []Match{match(DEVICE, DEVICES, INTERSECTS)},
			populate: func(refs reference) {
				_, _ = refs.devices.InsertOrReplace(context.TODO(),
					makeDevice("c5vj26evvhfjvfseauog", 42.9214863, -72.2759164))
			},
		},
		{
			spec: []string{
				`devices(c5vj26evvhfjvfseauk0) INTERSECTS devices(c5vj26evvhfjvfseauog) { :layer c5vj26evvhfjvfseaumg } `,
			},
			target: makeDevice("c5vj26evvhfjvfseauk0", 42.9214863, -72.2759164),
		},
		{
			err: true,
			spec: []string{
				`devices(@) INTERSECTS devices(@)`,
			},
		},
	}

	assertRuntimeTestCase(t, specs)
}

func TestRuntimeNearDevicesDevices(t *testing.T) {
	specs := []rTestCase{
		{ // @ <- all devices
			spec: []string{
				`devices(c5vj26evvhfjvfseauk0) :radius 500m NEAR devices(@)`,
				`devices(c5vj26evvhfjvfseauk0) :bbox 500m NEAR devices(@) :bbox 10m`,
				`devices(@) NEAR devices(c5vj26evvhfjvfseauk0) :radius 500m`,
			},
			target: makeDevice("c5vj26evvhfjvfseauk0", 42.9214863, -72.2794802),
			match:  []Match{match(DEVICE, DEVICES, NEAR)},
			populate: func(refs reference) {
				_, _ = refs.devices.InsertOrReplace(context.TODO(),
					makeDevice("c5vj26evvhfjvfseauog", 42.9240239, -72.2787075))
				_, _ = refs.devices.InsertOrReplace(context.TODO(),
					makeDevice("c5vj26evvhfjvfseaukg", 42.9226333, -72.2732452))
			},
		},
		{
			spec: []string{
				`device NEAR devices(@)`,
			},
			target: makeDevice("c5vj26evvhfjvfseauk0", 42.9214863, -72.2794802),
			match:  []Match{match(DEVICE, DEVICES, NEAR)},
			populate: func(refs reference) {
				_, _ = refs.devices.InsertOrReplace(context.TODO(),
					makeDevice("c5vj26evvhfjvfseauog", 42.9214863, -72.2794802))
				_, _ = refs.devices.InsertOrReplace(context.TODO(),
					makeDevice("c5vj26evvhfjvfseaukg", 42.9214863, -72.2794802))
			},
		},
	}

	assertRuntimeTestCase(t, specs)
}

func assertRuntimeTestCase(t *testing.T, cases []rTestCase) {
	for i, tc := range cases {
		refs := defaultRefs()
		if tc.populate != nil {
			tc.populate(refs)
		}
		for _, specstr := range tc.spec {
			spec, err := specFromString(specstr)
			if err != nil {
				if tc.err {
					return
				} else {
					t.Fatalf("specFromString(%s) => error %v", specstr, err)
				}
			} else if tc.err {
				t.Fatalf("specFromString(%s) => got nil, expected err", specstr)
			}
			ruleID := xid.New()
			matches, _, err := spec.evaluate(context.TODO(), ruleID, tc.target, refs)
			if err != nil {
				t.Fatal(err)
			}
			if have, want := len(matches), len(tc.match); have != want {
				t.Fatalf("%d specFromString(%s) => got %v, expected %v matching", i, specstr, have, want)
			}
			for i, m := range matches {
				if have, want := m.Ok, tc.match[i].Ok; have != want {
					t.Fatalf("specFromString(%s) => got %v, expected %v matches", specstr, have, want)
				}
				if have, want := m.Left.Keyword, tc.match[i].Left.Keyword; have != want {
					t.Fatalf("specFromString(%s) => got %v, expected %v left keyword", specstr, have, want)
				}
				if have, want := m.Right.Keyword, tc.match[i].Right.Keyword; have != want {
					t.Fatalf("specFromString(%s) => got %v, expected %v right keyword", specstr, have, want)
				}
				if have, want := m.Operator, tc.match[i].Operator; have != want {
					t.Fatalf("specFromString(%s) => got %v, expected %v left keyword", specstr, have, want)
				}
			}
		}
	}
}

func str2obj(id string, coords string) *GeoObject {
	obj := polyFromString(coords)
	cid, _ := xid.FromString(id)
	return NewGeoObject(cid, DefaultLayer, obj)
}

func did(id string) xid.ID {
	deviceID, _ := xid.FromString(id)
	return deviceID
}

func makeDevice(id string, lat, lon float64) *Device {
	return &Device{ID: did(id), Latitude: lat, Longitude: lon}
}

func match(left, right, op Token) Match {
	return Match{
		Ok:       true,
		Left:     Decl{Keyword: left},
		Right:    Decl{Keyword: right},
		Operator: op,
	}
}
