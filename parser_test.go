package spinix

import (
	"fmt"
	"strings"
	"testing"
)

func TestParser(t *testing.T) {
	testCases := []struct {
		spec  string
		isErr bool
	}{
		// successfully
		{spec: `device near polygon(@id) :time duration 5m0s`},
		{spec: `device near polygon(@id) :time after 5m0s`},
		{spec: `circle(@id) :time duration 5s near device :radius 5km`},
		{spec: `devices(@id, @id2) :bbox 300m near devices(@id, @id) :bbox 400m`},
		{spec: `device :radius 300m intersects devices(@id, @id) :radius 400m`},
		{spec: `device not intersects rect(@id)`},
		{spec: `device not intersects rect(@1, @2, @3)`},
		{spec: `objects(@id) contains device :bbox 4m`},
		{spec: `device :radius 2.4km within circle(@home) :time duration 10m`},
		{spec: `objects(@id) contains device`},
		{spec: `speed range [1 .. 60]`},
		{spec: `temperature range [2.2 .. 10.8]`},
		{spec: `temperature >= 1 and temperature < 40`},
		{spec: `pressure >= 1 and pressure < 40`},
		{spec: `luminosity >= 1 and luminosity < 40`},
		{spec: `battery range [0 .. 30]`},
		{spec: `fuelLevel range [0 .. 30]`},
		{spec: `status range [0 .. 30]`},
		{spec: `humidity range [0 .. 30]`},
		{spec: `imei in ["one", "two"]`},
		{spec: `year range [2022 .. 2023]`},
		{spec: `month range [1 .. 12]`},
		{spec: `week in [48, 49, 50] and week range [40 .. 52]`},
		{spec: `day range [1 .. 12]`},

		{spec: `device :radius 300m intersects line(@id) and speed range [30 .. 120]`},

		// failure
		{spec: "", isErr: true},
		{spec: "some text", isErr: true},
		{spec: `circle() intersects device`, isErr: true},
		{spec: `circle intersects device`, isErr: true},
		{spec: `circle(....) intersects device`, isErr: true},
		{spec: `circle(@id, @"test", "test") intersects device`, isErr: true},
		{spec: `device near polygon(@id) :time duration h3s`, isErr: true},
		{spec: fmt.Sprintf(`device near polygon(@%s) :time duration h3s`, strings.Repeat("o", 128)), isErr: true},
		{spec: `objects(@id) contains device :bbox 4meters`, isErr: true},
		{spec: `device :`, isErr: true},
		{spec: `device near polygon(@id) :time before 5m0s`, isErr: true},
		{spec: `device near polygon(@id) :time after`, isErr: true},
		{spec: `circle(@id) :time duration 5s near device :radius 5km`, isErr: true},
		{spec: `device :radius b0km`, isErr: true},
		{spec: `speed range [0x0 .. b0]`, isErr: true},
		{spec: `speed range [0x0 .. b0.0]`, isErr: true},
		{spec: `owner in []`, isErr: true},
		{spec: `brand in [1 .. 2, 1, 3]`, isErr: true},
		{spec: `model in [1 ... 2]`, isErr: true},
		{spec: `iemi in [1, 1.1, "one"]`, isErr: true},
		{spec: `owner in [1.1, "one", 1]`, isErr: true},
		{spec: `owner in ["one", 1.1, 1]`, isErr: true},
		{spec: `owner in [1.1, 1]`, isErr: true},
	}
	for _, tc := range testCases {
		expr, err := ParseSpec(tc.spec)
		if err != nil {
			if tc.isErr {
				continue
			} else {
				t.Fatal(err)
			}
		}
		if expr == nil {
			t.Fatalf("ParseSpec(%s) => nil, want Expr", tc.spec)
		}
	}
}
