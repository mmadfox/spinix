package spinix

import (
	"context"
	"log"
	"testing"
)

var poly = polyFromString(`
-72.2808218, 42.9279834
-72.2805106, 42.9266950
-72.2789867, 42.9268207
-72.2792657, 42.9282269
-72.2808218, 42.9280226
-72.2808218, 42.9279834
`)

func TestNearOpDeviceObjectWithoutRadius(t *testing.T) {
	ctx := context.Background()
	refs := defaultRefs()
	if err := refs.objects.Add(ctx, "poly", poly); err != nil {
		t.Fatal(err)
	}

	spec, err := specFromString(`device near objects(@poly)`)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		lat, lon float64
		ok       bool
		isErr    bool
	}{
		{lat: 42.927292, lon: -72.2814013, ok: false},
		{lat: 42.9272999, lon: -72.2810793, ok: false},
		{lat: 42.9273156, lon: -72.2807789, ok: false},
		{lat: 42.9273549, lon: -72.2803925, ok: true},
		{lat: 42.9273706, lon: -72.2796306, ok: true},
		{lat: 42.9275022, lon: -72.2791461, ok: true},
	}

	for _, tc := range testCases {
		device := &Device{
			IMEI:      "id",
			Latitude:  tc.lat,
			Longitude: tc.lon,
		}
		matches, err := spec.invoke(ctx, device, refs)
		if err != nil {
			if tc.isErr {
				continue
			}
			t.Fatal(err)
		}
		if hasMatches(matches) != tc.ok {
			t.Fatalf("spec: %v => have %v, want %v", tc, hasMatches(matches), tc.ok)
		}
	}
}

func TestNearOpDeviceObjectWithRadius100meters(t *testing.T) {
	ctx := context.Background()
	refs := defaultRefs()

	if err := refs.objects.Add(ctx, "poly", poly); err != nil {
		t.Fatal(err)
	}

	spec, err := specFromString(`device :radius 100m near objects(@poly)`)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		lat, lon float64
		ok       bool
		isErr    bool
	}{
		{lat: 42.9277084, lon: -72.2834507, ok: false},
		{lat: 42.9276456, lon: -72.2815405, ok: true},
		{lat: 42.9273156, lon: -72.2807789, ok: true},
		{lat: 42.9273549, lon: -72.2803925, ok: true},
		{lat: 42.9273706, lon: -72.2796306, ok: true},
		{lat: 42.9275022, lon: -72.2791461, ok: true},
	}

	for _, tc := range testCases {
		device := &Device{
			Latitude:  tc.lat,
			Longitude: tc.lon,
		}
		matches, err := spec.invoke(ctx, device, refs)
		if err != nil {
			if tc.isErr {
				continue
			}
			t.Fatal(err)
		}
		if hasMatches(matches) != tc.ok {
			t.Fatalf("spec: %v => have %v, want %v", tc, hasMatches(matches), tc.ok)
		}
	}
}

func TestNearOpDeviceObjectWithBBox2kilometers(t *testing.T) {
	ctx := context.Background()
	refs := defaultRefs()
	if err := refs.objects.Add(ctx, "poly", poly); err != nil {
		t.Fatal(err)
	}

	spec, err := specFromString(`device :bbox 2km near objects(@poly)`)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		lat, lon float64
		ok       bool
		isErr    bool
	}{
		{lat: 42.9400251, lon: -72.3141553, ok: false},
		{lat: 42.944172, lon: -72.2519128, ok: false},
		{lat: 42.9273156, lon: -72.2807789, ok: true},
		{lat: 42.9273549, lon: -72.2803925, ok: true},
		{lat: 42.9273706, lon: -72.2796306, ok: true},
		{lat: 42.9275022, lon: -72.2791461, ok: true},
	}

	for _, tc := range testCases {
		device := &Device{
			Latitude:  tc.lat,
			Longitude: tc.lon,
		}
		matches, err := spec.invoke(ctx, device, refs)
		if err != nil {
			if tc.isErr {
				continue
			}
			t.Fatal(err)
		}
		if have, want := hasMatches(matches), tc.ok; have != want {
			t.Fatalf("spec: %v => have %v, want %v", tc, have, want)
		}
	}
}

func TestNearOpDeviceDevicesWithoutRadius(t *testing.T) {
	ctx := context.Background()
	refs := defaultRefs()

	spec, err := specFromString(`device near devices(@deviceOther)`)
	if err != nil {
		t.Fatal(err)
	}

	// devices
	deviceOtherLatLon := []struct {
		lat, lon float64
	}{
		{lat: 42.9270956, lon: -72.2798013},
		{lat: 42.9258858, lon: -72.2784706},
	}

	deviceLatLon := []struct {
		lat, lon float64
		near     bool
	}{
		{lat: 42.9270956, lon: -72.2798013, near: true},
		{lat: 42.9256816, lon: -72.2783633, near: false},
	}

	for i := 0; i < len(deviceLatLon); i++ {
		// current device
		device := &Device{
			IMEI:      "device",
			Latitude:  deviceLatLon[i].lat,
			Longitude: deviceLatLon[i].lon,
		}
		// other device
		deviceOther := &Device{
			IMEI:      "deviceOther",
			Latitude:  deviceOtherLatLon[i].lat,
			Longitude: deviceOtherLatLon[i].lon,
		}
		if err := refs.devices.InsertOrReplace(ctx, deviceOther); err != nil {
			t.Fatal(err)
		}

		matches, err := spec.invoke(ctx, device, refs)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := deviceLatLon[i].near, hasMatches(matches); have != want {
			t.Fatalf("spec: %v => have %v, want %v", spec, have, want)
		}
	}
}

func TestNearOpDeviceDevicesWithRadius100meters(t *testing.T) {
	ctx := context.Background()
	refs := defaultRefs()

	specBBox, err := specFromString(`device :radius 100m near devices(@deviceOther) :bbox 100m`)
	if err != nil {
		t.Fatal(err)
	}

	specRadius, err := specFromString(`device :radius 100m near devices(@deviceOther) :radius 100m`)
	if err != nil {
		t.Fatal(err)
	}

	// devices
	deviceOtherLatLon := []struct {
		lat, lon float64
	}{
		{lat: 42.9214706, lon: -72.2758521},
		{lat: 42.928494, lon: -72.2772901},
	}

	deviceLatLon := []struct {
		lat, lon float64
		near     bool
	}{
		{lat: 42.9309292, lon: -72.2844587, near: false},
		{lat: 42.9285568, lon: -72.2775906, near: true},
	}

	for i := 0; i < len(deviceLatLon); i++ {
		// current device
		device := &Device{
			IMEI:      "device",
			Latitude:  deviceLatLon[i].lat,
			Longitude: deviceLatLon[i].lon,
		}
		// other device
		deviceOther := &Device{
			IMEI:      "deviceOther",
			Latitude:  deviceOtherLatLon[i].lat,
			Longitude: deviceOtherLatLon[i].lon,
		}
		if err := refs.devices.InsertOrReplace(ctx, deviceOther); err != nil {
			t.Fatal(err)
		}

		matches, err := specBBox.invoke(ctx, device, refs)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := deviceLatLon[i].near, hasMatches(matches); have != want {
			t.Fatalf("specBBox: %v => have %v, want %v", deviceLatLon[i], have, want)
		}

		matches, err = specRadius.invoke(ctx, device, refs)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := deviceLatLon[i].near, hasMatches(matches); have != want {
			t.Fatalf("specRadius: %v => have %v, want %v", deviceLatLon[i], have, want)
		}
	}
}

func TestNearOpDeviceDeviceWithRadius100meters(t *testing.T) {
	ctx := context.Background()
	refs := defaultRefs()

	spec, err := specFromString(`device :radius 100m near device`)
	if err != nil {
		t.Fatal(err)
	}

	if err := refs.devices.InsertOrReplace(ctx, &Device{
		IMEI:      "one",
		Latitude:  42.9328852,
		Longitude: -72.2764333,
	}); err != nil {
		t.Fatal(err)
	}

	if err := refs.devices.InsertOrReplace(ctx, &Device{
		IMEI:      "two",
		Latitude:  42.9326731,
		Longitude: -72.2755318,
	}); err != nil {
		t.Fatal(err)
	}

	matches, err := spec.invoke(ctx, &Device{
		IMEI:      "three",
		Latitude:  42.9327438,
		Longitude: -72.2759504,
	}, refs)

	log.Println(matches)

	_ = matches
}

func TestNearOpDeviceMultiObjects(t *testing.T) {
	ctx := context.Background()
	refs := defaultRefs()

	if err := refs.objects.Add(ctx, "poly1", poly); err != nil {
		t.Fatal(err)
	}
	if err := refs.objects.Add(ctx, "poly2", poly); err != nil {
		t.Fatal(err)
	}
	if err := refs.objects.Add(ctx, "poly3", poly); err != nil {
		t.Fatal(err)
	}

	spec, err := specFromString(`
     (device :radius 10m near objects(@poly1) AND device :radius 10m near objects(@poly2)) 
     OR (device :radius 1km near objects(@poly3))
`)
	if err != nil {
		t.Fatal(err)
	}

	matching, err := spec.invoke(ctx, &Device{
		Latitude:  42.9261765,
		Longitude: -72.2796643,
	}, refs)
	if err != nil {
		t.Fatal(err)
	}
	_ = matching
}

func TestRangeOp(t *testing.T) {
	testCases := []struct {
		spec string
		err  bool
	}{
		// successfully
		{
			spec: `datetime range ["2012-11-01T22:08:41+00:00" .. "2012-12-01T22:08:41+00:00"]`,
		},
		{
			spec: `datetime range ["2012-11-01T22:08:41+00:00" .. "2012-12-01T22:08:41+00:00"]`,
		},
		{
			spec: `date range ["2012-11-01" .. "2012-12-02"]`,
		},
		{
			spec: `time range [01:00 .. 23:50]`,
		},
		{
			spec: `speed range [1 .. 30]`,
		},
		{
			spec: `fuelLevel range [1 .. 2]`,
		},
		{
			spec: `fuelLevel range [1.1 .. 2.1]`,
		},

		// failure
		{
			spec: `datetime range [12:12 .. 14:45]`,
			err:  true,
		},
		{
			// left == right
			spec: `datetime range ["2012-11-01T22:08:41+00:00" .. "2012-11-01T22:08:41+00:00"]`,
			err:  true,
		},
		{
			// left == right
			spec: `datetime range ["2012-11-01" .. "2012-11-01"]`,
			err:  true,
		},
		{
			// left > right
			spec: `datetime range ["2012-12-01T22:08:41+00:00" .. "2012-11-01T22:08:41+00:00"]`,
			err:  true,
		},
		{
			spec: `datetime range ["" .. ""]`,
			err:  true,
		},
		{
			spec: `datetime range ["" .. ""]`,
			err:  true,
		},
		{
			spec: `datetime range ["1" .. "2"]`,
			err:  true,
		},
		{
			spec: `datetime range []`,
			err:  true,
		},
		{
			spec: `time range [333:333 .. 1111:55555]`,
			err:  true,
		},
		{
			spec: `time range [11:333 .. 1111:55555]`,
			err:  true,
		},
		{
			spec: `time range [11:11 .. 1111:55555]`,
			err:  true,
		},
		{
			spec: `time range [11:11 .. 11:55555]`,
			err:  true,
		},
		{
			spec: `time range [1 .. 30]`,
			err:  true,
		},
		{
			spec: `time range [1.0 .. 30.0]`,
			err:  true,
		},
		{
			spec: `fuelLevel range [2 .. 1]`,
			err:  true,
		},
		{
			spec: `fuelLevel range [2 .. 2]`,
			err:  true,
		},
		{
			spec: `fuelLevel range [3.2 .. 1.0]`,
			err:  true,
		},
		{
			spec: `fuelLevel range [2.0 .. 2.0]`,
			err:  true,
		},
	}
	for _, tc := range testCases {
		_, err := specFromString(tc.spec)
		if err != nil {
			if tc.err {
				continue
			} else {
				t.Fatalf("specFromString(%s) => %v", tc.spec, err)
			}
		} else if tc.err {
			t.Fatalf("specFromString(%s) => got nil, expected err", tc.spec)
		}
	}
}
