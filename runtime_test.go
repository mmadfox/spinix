package spinix

import (
	"context"
	"log"
	"testing"
	"time"
)

var poly = polyFromString(`
-72.2808218, 42.9279834
-72.2805106, 42.9266950
-72.2789867, 42.9268207
-72.2792657, 42.9282269
-72.2808218, 42.9280226
-72.2808218, 42.9279834
`)

func TestTriggerRepeatEvery(t *testing.T) {
	t.Skip()
	ctx := context.Background()
	refs := defaultRefs()
	_ = refs.objects.Add(ctx, "poly", poly)
	spec, err := specFromString(`device intersects objects(@poly) :trigger every 2s`)
	if err != nil {
		t.Fatal(err)
	}
	var trigger int
	for i := 0; i < 7; i++ {
		_, ok, err := spec.evaluate(ctx, "test", &Device{
			IMEI:      "one",
			Latitude:  42.927457,
			DateTime:  time.Now().Unix(),
			Longitude: -72.2798688}, refs)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			trigger++
		}
		time.Sleep(time.Second)
	}
	if trigger != 3 {
		t.Fatalf("got %d, expected 3", trigger)
	}
}

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
		matches, _, err := spec.evaluate(ctx, "", device, refs)
		if err != nil {
			if tc.isErr {
				continue
			}
			t.Fatal(err)
		}
		if hasMatches(matches) != tc.ok {
			t.Fatalf("specStr: %v => have %v, want %v", tc, hasMatches(matches), tc.ok)
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
		matches, _, err := spec.evaluate(ctx, "", device, refs)
		if err != nil {
			if tc.isErr {
				continue
			}
			t.Fatal(err)
		}
		if hasMatches(matches) != tc.ok {
			t.Fatalf("specStr: %v => have %v, want %v", tc, hasMatches(matches), tc.ok)
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
		matches, _, err := spec.evaluate(ctx, "", device, refs)
		if err != nil {
			if tc.isErr {
				continue
			}
			t.Fatal(err)
		}
		if have, want := hasMatches(matches), tc.ok; have != want {
			t.Fatalf("specStr: %v => have %v, want %v", tc, have, want)
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
		if _, err := refs.devices.InsertOrReplace(ctx, deviceOther); err != nil {
			t.Fatal(err)
		}

		matches, _, err := spec.evaluate(ctx, "", device, refs)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := deviceLatLon[i].near, hasMatches(matches); have != want {
			t.Fatalf("specStr: %v => have %v, want %v", spec, have, want)
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
		if _, err := refs.devices.InsertOrReplace(ctx, deviceOther); err != nil {
			t.Fatal(err)
		}

		matches, _, err := specBBox.evaluate(ctx, "", device, refs)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := deviceLatLon[i].near, hasMatches(matches); have != want {
			t.Fatalf("specBBox: %v => have %v, want %v", deviceLatLon[i], have, want)
		}

		matches, _, err = specRadius.evaluate(ctx, "", device, refs)
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

	if _, err := refs.devices.InsertOrReplace(ctx, &Device{
		IMEI:      "one",
		Latitude:  42.9328852,
		Longitude: -72.2764333,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := refs.devices.InsertOrReplace(ctx, &Device{
		IMEI:      "two",
		Latitude:  42.9326731,
		Longitude: -72.2755318,
	}); err != nil {
		t.Fatal(err)
	}

	matches, _, err := spec.evaluate(ctx, "", &Device{
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

	matching, _, err := spec.evaluate(ctx, "", &Device{
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
		d    *Device
		m    []Match
		err  bool
		ok   bool
	}{
		// successfully
		{
			spec: `time range [12:00 .. 23:00]`,
			d:    &Device{DateTime: 1634839200},
			m:    []Match{_mm(TIME, TIME, RANGE)},
			ok:   true,
		},
		{
			spec: `time nrange [12:00 .. 13:00]`,
			d:    &Device{DateTime: 1634839200},
			m:    []Match{_mm(TIME, TIME, NRANGE)},
			ok:   true,
		},
		{
			spec: `speed nrange [30 .. 60]`,
			d:    &Device{Speed: 25},
			m:    []Match{_mm(SPEED, INT, NRANGE)},
			ok:   true,
		},
		{
			spec: `speed nrange [10 .. 60]`,
			d:    &Device{Speed: 25},
			ok:   false,
		},
		{
			spec: `time range [21:01 .. 23:00]`,
			d:    &Device{DateTime: 1634839200},
			ok:   false,
		},
		{
			spec: `speed range [1 .. 20]`,
			d:    &Device{Speed: 19},
			m:    []Match{_mm(SPEED, INT, RANGE)},
			ok:   true,
		},
		{
			spec: `speed range [1.1 .. 20.5]`,
			d:    &Device{Speed: 19},
			m:    []Match{_mm(SPEED, FLOAT, RANGE)},
			ok:   true,
		},

		{
			spec: `time range [21:01 .. 23.00]`,
			err:  true,
		},
		{
			spec: `speed range [1,2,3]`,
			err:  true,
		},
		{
			spec: `owner range [1 .. 2]`,
			err:  true,
		},
		{
			spec: `owner range [1.0 .. 2.1]`,
			err:  true,
		},
		{
			spec: `speed range [2 .. 1]`,
			err:  true,
		},
		{
			spec: `speed range [2 .. 2]`,
			err:  true,
		},
		{
			spec: `speed range [2.0 .. 1.0]`,
			err:  true,
		},
		{
			spec: `speed range [2.0 .. 2.0]`,
			err:  true,
		},
	}
	refs := defaultRefs()
	for _, tc := range testCases {
		spec, err := specFromString(tc.spec)
		if err != nil {
			if tc.err {
				continue
			} else {
				t.Fatalf("specFromString(%s) => %v", tc.spec, err)
			}
		} else if tc.err {
			t.Fatalf("specFromString(%s) => got nil, expected err", tc.spec)
		}
		matches, ok, err := spec.evaluate(context.Background(), "", tc.d, refs)
		if err != nil {
			t.Fatal(err)
		}
		if tc.ok != ok {
			t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, ok, tc.ok)
		}
		if have, want := len(matches), len(tc.m); have != want {
			t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
		}
		for i, m := range matches {
			if have, want := m.Ok, tc.m[i].Ok; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Left.Keyword, tc.m[i].Left.Keyword; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Right.Keyword, tc.m[i].Right.Keyword; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Operator, tc.m[i].Operator; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
		}
	}
}

func TestIntersectsOp(t *testing.T) {
	testCases := []struct {
		spec string
		d    *Device
		m    []Match
		err  bool
	}{
		{
			spec: `device :radius 100m intersects devices(@other) :radius 100m`,
			d:    &Device{Latitude: 42.9284788, Longitude: -72.2776118},
			m:    []Match{_mm(DEVICE, DEVICES, INTERSECTS)},
		},
		{
			spec: `device :radius 100m nintersects devices(@other) :radius 100m`,
			d:    &Device{Latitude: 42.9276186, Longitude: -72.2798106},
			m:    []Match{_mm(DEVICE, DEVICES, NINTERSECTS)},
		},
		{
			spec: `device intersects polygon(@poly1)`,
			d:    &Device{Latitude: 42.9272263, Longitude: -72.2796414},
			m:    []Match{_mm(DEVICE, POLY, INTERSECTS)},
		},
		{
			spec: `polygon(@poly1) intersects device`,
			d:    &Device{Latitude: 42.9272263, Longitude: -72.2796414},
			m:    []Match{_mm(DEVICE, POLY, INTERSECTS)},
		},
		{
			spec: `device nintersects polygon(@poly1)`,
			d:    &Device{Latitude: 42.9285063, Longitude: -72.2776171},
			m:    []Match{_mm(DEVICE, POLY, NINTERSECTS)},
		},
		{
			spec: `polygon(@poly1) nintersects device`,
			d:    &Device{Latitude: 42.9285063, Longitude: -72.2776171},
			m:    []Match{_mm(DEVICE, POLY, NINTERSECTS)},
		},
		{
			spec: `device :radius 300m intersects polygon(@poly1)`,
			d:    &Device{Latitude: 42.9265468, Longitude: -72.2795556},
			m:    []Match{_mm(DEVICE, POLY, INTERSECTS)},
		},
		{
			spec: `device :bbox 1km intersects polygon(@poly1)`,
			d:    &Device{Latitude: 42.9291859, Longitude: -72.2765499},
			m:    []Match{_mm(DEVICE, POLY, INTERSECTS)},
		},
	}

	ctx := context.Background()
	refs := defaultRefs()
	_ = refs.objects.Add(ctx, "poly1", poly)
	_, _ = refs.devices.InsertOrReplace(ctx, &Device{IMEI: "other", Latitude: 42.9287184, Longitude: -72.2778048})

	for _, tc := range testCases {
		spec, err := specFromString(tc.spec)
		if err != nil {
			if tc.err {
				continue
			} else {
				t.Fatalf("specFromString(%s) => %v", tc.spec, err)
			}
		} else if tc.err {
			t.Fatalf("specFromString(%s) => got nil, expected err", tc.spec)
		}
		matches, _, err := spec.evaluate(ctx, "", tc.d, refs)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := len(matches), len(tc.m); have != want {
			t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
		}
		for i, m := range matches {
			if have, want := m.Ok, tc.m[i].Ok; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Left.Keyword, tc.m[i].Left.Keyword; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Right.Keyword, tc.m[i].Right.Keyword; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Operator, tc.m[i].Operator; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
		}
	}
}

func TestInOp(t *testing.T) {
	testCases := []struct {
		spec string
		d    *Device
		m    []Match
		err  bool
	}{
		// successfully
		{
			spec: `device :radius 10m in polygon(@poly1)`,
			d:    &Device{Latitude: 42.927512, Longitude: -72.2798742},
			m:    []Match{_mm(DEVICE, POLY, IN)},
		},
		{
			spec: `device :bbox 40m in polygon(@poly1)`,
			d:    &Device{Latitude: 42.927512, Longitude: -72.2798742},
			m:    []Match{_mm(DEVICE, POLY, IN)},
		},
		{
			spec: `device in polygon(@poly1)`,
			d:    &Device{Latitude: 42.927512, Longitude: -72.2798742},
			m:    []Match{_mm(DEVICE, POLY, IN)},
		},
		{
			spec: `device nin polygon(@poly1)`,
			d:    &Device{Latitude: 42.9273235, Longitude: -72.2823695},
			m:    []Match{_mm(DEVICE, POLY, NIN)},
		},
		{
			spec: `imei in ["one", "two", "three three"] and speed in [60]`,
			d:    &Device{IMEI: "one", Speed: 60},
			m:    []Match{_mm(IMEI, STRING, IN), _mm(SPEED, INT, IN)},
		},
		{
			spec: `imei nin ["one", "two", "three three"] and speed in [60]`,
			d:    &Device{IMEI: "bad", Speed: 60},
			m:    []Match{_mm(IMEI, STRING, NIN), _mm(SPEED, INT, IN)},
		},
		{
			spec: `model in [one, two, three] or imei in ["ONE"]`,
			d:    &Device{Model: "one"},
			m:    []Match{_mm(MODEL, STRING, IN)},
		},
		{
			spec: `status in [1, 2, 3] or status in [1.0]`,
			d:    &Device{Status: 1},
			m:    []Match{_mm(STATUS, INT, IN), _mm(STATUS, FLOAT, IN)},
		},
		{
			spec: `status nin [1, 2, 3]`,
			d:    &Device{Status: 10},
			m:    []Match{_mm(STATUS, INT, NIN)},
		},
		{
			spec: `speed in [1.1, 2.1, 3.1]`,
			d:    &Device{Speed: 1.1},
			m:    []Match{_mm(SPEED, FLOAT, IN)},
		},
		{
			spec: `day in [21, 55, 124] and month in [10]`,
			d:    &Device{DateTime: 1634774886},
			m:    []Match{_mm(DAY, INT, IN), _mm(MONTH, INT, IN)},
		},
		{
			spec: `fuelLevel in [21] or fuelLevel in [21.0]`,
			d:    &Device{FuelLevel: 21},
			m:    []Match{_mm(FUELLEVEL, INT, IN), _mm(FUELLEVEL, FLOAT, IN)},
		},
		{
			spec: `pressure in [21] or pressure in [21.0]`,
			d:    &Device{Pressure: 21},
			m:    []Match{_mm(PRESSURE, INT, IN), _mm(PRESSURE, FLOAT, IN)},
		},
		{
			spec: `luminosity in [21] or luminosity in [21.0]`,
			d:    &Device{Luminosity: 21},
			m:    []Match{_mm(LUMINOSITY, INT, IN), _mm(LUMINOSITY, FLOAT, IN)},
		},
		{
			spec: `humidity in [21] or humidity in [21.0]`,
			d:    &Device{Humidity: 21},
			m:    []Match{_mm(HUMIDITY, INT, IN), _mm(HUMIDITY, FLOAT, IN)},
		},
		{
			spec: `temperature in [21] or temperature in [21.0]`,
			d:    &Device{Temperature: 21},
			m:    []Match{_mm(TEMPERATURE, INT, IN), _mm(TEMPERATURE, FLOAT, IN)},
		},
		{
			spec: `battery in [21] or battery in [21.0]`,
			d:    &Device{BatteryCharge: 21},
			m:    []Match{_mm(BATTERY_CHARGE, INT, IN), _mm(BATTERY_CHARGE, FLOAT, IN)},
		},
		{
			spec: `
              (model in [one]) or (brand in ["one-one"]) or 
              (owner in ["40c34e6c-c3c1-4226-bfea-7995336c9a9e"]) or 
              (imei in ["40c34e6c-c3c1-4226-bfea-7995336c9a9e"])`,
			d: &Device{Model: "one", Brand: "one-one", Owner: "40c34e6c-c3c1-4226-bfea-7995336c9a9e", IMEI: "40c34e6c-c3c1-4226-bfea-7995336c9a9e"},
			m: []Match{_mm(MODEL, STRING, IN), _mm(BRAND, STRING, IN), _mm(OWNER, STRING, IN), _mm(IMEI, STRING, IN)},
		},
		{
			spec: `year in [2021] or year in [2021.0]`,
			d:    &Device{DateTime: 1634774886},
			m:    []Match{_mm(YEAR, INT, IN), _mm(YEAR, FLOAT, IN)},
		},
		{
			spec: `month in [10] or month in [10.0]`,
			d:    &Device{DateTime: 1634774886},
			m:    []Match{_mm(MONTH, INT, IN), _mm(MONTH, FLOAT, IN)},
		},

		{
			spec: `week in [42] or week in [42.0]`,
			d:    &Device{DateTime: 1634774886},
			m:    []Match{_mm(WEEK, INT, IN), _mm(WEEK, FLOAT, IN)},
		},

		{
			spec: `day in [21] or day in [21.0]`,
			d:    &Device{DateTime: 1634774886},
			m:    []Match{_mm(DAY, INT, IN), _mm(DAY, FLOAT, IN)},
		},
		{
			spec: `hour in [03] or hour in [03.0]`,
			d:    &Device{DateTime: 1634774886},
			m:    []Match{_mm(HOUR, INT, IN), _mm(HOUR, FLOAT, IN)},
		},

		{
			spec: `month in [October]`,
			d:    &Device{DateTime: 1634774886},
			m:    []Match{_mm(MONTH, STRING, IN)},
		},
		{
			spec: `day in [Thursday]`,
			d:    &Device{DateTime: 1634774886},
			m:    []Match{_mm(DAY, STRING, IN)},
		},
		{
			spec: `date in ["2021-10-21"]`,
			d:    &Device{DateTime: 1634774886},
			m:    []Match{_mm(DATE, STRING, IN)},
		},
		{
			spec: `datetime in ["2021-10-21T03:08:06+03:00"]`,
			d:    &Device{DateTime: 1634774886},
			m:    []Match{_mm(DATETIME, STRING, IN)},
		},

		// failure
		{spec: `1 in 1`, err: true},
		{spec: `status in [1, "two" , three]`, err: true},
		{spec: `device in [1]`, err: true},
		{spec: `objects(@id) in [1]`, err: true},
		{spec: `speed in []`, err: true},
		{spec: `speed in [1, 1.1, 2]`, err: true},
		{spec: `speed in [1 .. 2]`, err: true},
		{spec: `time in [12:00, 13:00, 14:00]`, err: true},
		{spec: `model in [1,2,3]`, err: true},
		{spec: `imei in [2.2,4.4,3.3]`, err: true},
		{spec: `speed in [one, two]`, err: true},
		{spec: `week in [one]`, err: true},
	}

	ctx := context.Background()
	refs := defaultRefs()
	_ = refs.objects.Add(ctx, "poly1", poly)

	for _, tc := range testCases {
		spec, err := specFromString(tc.spec)
		if err != nil {
			if tc.err {
				continue
			} else {
				t.Fatalf("specFromString(%s) => %v", tc.spec, err)
			}
		} else if tc.err {
			t.Fatalf("specFromString(%s) => got nil, expected err", tc.spec)
		}
		matches, _, err := spec.evaluate(ctx, "", tc.d, refs)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := len(matches), len(tc.m); have != want {
			t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
		}
		for i, m := range matches {
			if have, want := m.Ok, tc.m[i].Ok; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Left.Keyword, tc.m[i].Left.Keyword; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Right.Keyword, tc.m[i].Right.Keyword; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Operator, tc.m[i].Operator; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
		}
	}
}

func TestEqualOp(t *testing.T) {
	testCases := []struct {
		spec string
		d    *Device
		m    []Match
		err  bool
	}{
		// successfully
		{
			spec: `device :radius 200m eq polygon(@poly)`,
			d:    &Device{Latitude: 42.9273235, Longitude: -72.2823695},
			m:    []Match{_mm(DEVICE, POLY, EQ)},
		},
		{
			spec: `device :radius 200m ne polygon(@poly)`,
			d:    &Device{Latitude: 42.9273235, Longitude: -72.2823695},
		},
		{
			spec: `device :radius 200m gte polygon(@poly)`,
			d:    &Device{Latitude: 42.9273235, Longitude: -72.2823695},
			m:    []Match{_mm(DEVICE, POLY, GTE)},
		},
		{
			spec: `device :radius 200m lte polygon(@poly)`,
			d:    &Device{Latitude: 42.9273235, Longitude: -72.2823695},
			m:    []Match{_mm(DEVICE, POLY, LTE)},
		},
		{
			spec: `speed gte 10 and speed lte 50`,
			d:    &Device{Speed: 51},
			m:    []Match{_mm(SPEED, INT, GTE)},
		},
		{
			spec: `21:00 eq time OR time eq 21:00`,
			d:    &Device{DateTime: 1634839200},
			m:    []Match{_mm(TIME, TIME, EQ), _mm(TIME, TIME, EQ)},
		},
		{
			spec: `time lt 22:00`,
			d:    &Device{DateTime: 1634839200},
			m:    []Match{_mm(TIME, TIME, LT)},
		},
		{
			spec: `time lte 21:00`,
			d:    &Device{DateTime: 1634839200},
			m:    []Match{_mm(TIME, TIME, LTE)},
		},
		{
			spec: `time lte 23:59`,
			d:    &Device{DateTime: 1634839200},
			m:    []Match{_mm(TIME, TIME, LTE)},
		},
		{
			spec: `time lt 21:01`,
			d:    &Device{DateTime: 1634839200},
			m:    []Match{_mm(TIME, TIME, LT)},
		},
		{
			spec: `"91645c47-009f-4958-a3d1-34e8fbdce69d" eq imei OR imei eq "91645c47-009f-4958-a3d1-34e8fbdce69d"`,
			d:    &Device{IMEI: "91645c47-009f-4958-a3d1-34e8fbdce69d"},
			m:    []Match{_mm(IMEI, STRING, EQ), _mm(IMEI, STRING, EQ)},
		},
		{
			spec: `0.75 eq temperature OR temperature eq 0.75`,
			d:    &Device{Temperature: 0.75},
			m:    []Match{_mm(TEMPERATURE, FLOAT, EQ), _mm(TEMPERATURE, FLOAT, EQ)},
		},
		{
			spec: `1 eq status OR status eq 1`,
			d:    &Device{Status: 1},
			m:    []Match{_mm(STATUS, INT, EQ), _mm(STATUS, INT, EQ)},
		},

		// failure
		{spec: `21:00 eq speed`, err: true},
		{spec: `owner eq 21:00`, err: true},
		{spec: `date eq 21:00`, err: true},
		{spec: `datetime eq 21:00`, err: true},

		{spec: `"91645c47-009f-4958-a3d1-34e8fbdce69d" eq status`, err: true},
		{spec: `0.74 eq owner`, err: true},
		{spec: `"text" eq status`, err: true},

		{spec: `12 eq owner and owner eq 12`, err: true},
		{spec: `owner eq 12`, err: true},
		{spec: `brand eq 12.3`, err: true},
		{spec: `speed eq "someid"`, err: true},
		{spec: `device eq device`, err: true},
	}
	ctx := context.Background()
	refs := defaultRefs()
	_ = refs.objects.Add(ctx, "poly", poly)
	for _, tc := range testCases {
		spec, err := specFromString(tc.spec)
		if err != nil {
			if tc.err {
				continue
			} else {
				t.Fatalf("specFromString(%s) => %v", tc.spec, err)
			}
		} else if tc.err {
			t.Fatalf("specFromString(%s) => no matching rules: got nil, expected err", tc.spec)
		}
		matches, _, err := spec.evaluate(ctx, "", tc.d, refs)
		if err != nil {
			t.Fatal(err)
		}
		if have, want := len(matches), len(tc.m); have != want {
			t.Fatalf("specFromString(%s) => no matching rules: got %v, expected %v", tc.spec, have, want)
		}
		for i, m := range matches {
			if have, want := m.Ok, tc.m[i].Ok; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Left.Keyword, tc.m[i].Left.Keyword; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Right.Keyword, tc.m[i].Right.Keyword; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
			if have, want := m.Operator, tc.m[i].Operator; have != want {
				t.Fatalf("specFromString(%s) => got %v, expected %v", tc.spec, have, want)
			}
		}
	}
}

func _mm(left, right, op Token) Match {
	return Match{
		Ok:       true,
		Left:     Decl{Keyword: left},
		Right:    Decl{Keyword: right},
		Operator: op,
	}
}
