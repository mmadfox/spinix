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
		{
			spec: `devices(c5vj1kevvhfjur1l9gug, c5vj26evvhfjvfseauk0, c5vj26evvhfjvfseauo0) :radius 1km INTERSECTS objects(@) { :layer c5vj26evvhfjvfseauo0 }`,
		},
		{
			spec: `device INTERSECTS polygon("c5vj1kevvhfjur1l9gug") AND speed range [1 .. 40] { :center 42.9284788 72.2776118 }`,
		},
		{
			spec: `device :radius 1km intersects polygon(c5vj1kevvhfjur1l9gug) { :center 42.9284788 72.2776118 }`,
		},
		{
			spec: `devices(c5vj26evvhfjvfseauo0) :radius 100m near devices(@) :radius 100m { :trigger every 10s }`,
		},
		{
			spec: `devices(c5vj26evvhfjvfseauo0) :radius 100m near devices(c5vj1kevvhfjur1l9gug, c5vj26evvhfjvfseauk0, c5vj26evvhfjvfseauo0) :radius 100m { :trigger every 10s }`,
		},
		{
			spec: `device :radius 100m near devices(c5vj1kevvhfjur1l9gug, c5vj26evvhfjvfseauk0, c5vj26evvhfjvfseauo0) :radius 100m { :trigger every 10s }`,
		},
		{
			spec: `
                 status eq 1 OR 1 eq status 
                 {  
                    :radius 3km 
                    :center 42.4984338 -72.4265129 
                    :trigger every 10s 
                    :expire 10h 
                    :reset after 24h
                 }
`,
		},
		{spec: `device :radius 4km intersects polygon(c5vj26evvhfjvfseaulg) { :reset after 24h :trigger 25 times interval 10s }`},
		{spec: `device :radius 4km intersects polygon(c5vj26evvhfjvfseaulg) :trigger once :reset after 24h`},
		{spec: `device :radius 4km intersects polygon(c5vj26evvhfjvfseaulg) :reset after 24h :trigger every 10s`},
		{spec: `device :radius 4km intersects polygon(c5vj26evvhfjvfseaulg)`},
		{spec: `device intersects polygon(c5vj26evvhfjvfseaulg)`},
		{spec: `device :radius 4km in polygon(c5vj26evvhfjvfseaulg)`},
		{spec: `device :radius 4km nin polygon(c5vj26evvhfjvfseaulg)`},
		{spec: `status eq 1 OR 1 eq status`},
		{spec: `device near polygon(c5vj26evvhfjvfseaulg) :time duration 5m0s`},
		{spec: `device near polygon(c5vj26evvhfjvfseaulg) :time after 5m0s`},
		{spec: `circle(c5vj26evvhfjvfseaulg) :time duration 5s near device :radius 5km`},
		{spec: `devices(c5vj26evvhfjvfseaulg, c5vj26evvhfjvfseauo0) :bbox 300m near devices(c5vj26evvhfjvfseaulg, c5vj26evvhfjvfseauo0) :bbox 400m`},
		{spec: `device :radius 300m intersects devices(c5vj26evvhfjvfseaulg, c5vj26evvhfjvfseauo0) :radius 400m`},
		{spec: `speed range [1 .. 60]`},
		{spec: `speed nrange [1 .. 60]`},
		{spec: `temperature range [2.2 .. 10.8]`},
		{spec: `temperature gte 1 and temperature lt 40`},
		{spec: `pressure gte 1 and pressure lt 40`},
		{spec: `luminosity gte 1 and luminosity lt 40`},
		{spec: `battery range [0 .. 30]`},
		{spec: `fuelLevel range [0 .. 30]`},
		{spec: `status range [0 .. 30]`},
		{spec: `humidity range [0 .. 30]`},
		{spec: `imei in ["one", "two"]`},
		{spec: `year range [2022 .. 2023]`},
		{spec: `month range [1 .. 12]`},
		{spec: `week in [48, 49, 50] and week range [40 .. 52]`},
		{spec: `day range [1 .. 12]`},
		{spec: `time range [12:00 .. 23:00]`},
		{spec: `time gt 12:00 and time lt 15:00`},
		{spec: `time eq 19:21`},
		{spec: `datetime range ["2012-11-01T22:08:41+00:00" .. "2012-11-01T22:08:41+00:00"]`},
		{spec: `datetime gte "2012-11-01T22:08:41+00:00" and datetime lt "2012-11-01T22:08:41+00:00"`},
		{spec: `datetime in ["2012-11-01T22:08:41+00:00", "2012-11-01T22:08:41+00:00"]`},
		{spec: `device :radius 300m intersects line(c5vj26evvhfjvfseaum0) and speed range [30 .. 120]`},
		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] { :trigger 25 times interval 10s }`},
		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] { :trigger every 10s }`},
		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] { :trigger once }`},

		{spec: `device :radius 300m intersects line(c5vj26evvhfjvfseaum0) and speed range [30 .. 120]
			or (temperature gte 0 and temperature lt 400)`},

		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] :trigger`}, // ignore properties :trigger

		// failure
		{spec: "", isErr: true},
		{spec: "some text", isErr: true},
		{spec: `devices(,,,) intersects circle()`, isErr: true},
		{spec: `devices("c5vj26evvhfjvfseaum0") intersects circle()`, isErr: true},
		{spec: `circle() intersects device`, isErr: true},
		{spec: `circle intersects device`, isErr: true},
		{spec: `circle(....) intersects device`, isErr: true},
		{spec: `device near polygon(c5vj26evvhfjvfseaum0) :time duration h3s`, isErr: true},
		{spec: fmt.Sprintf(`device near polygon(@%s) :time duration h3s`, strings.Repeat("o", 128)), isErr: true},
		{spec: `device near polygon(c5vj26evvhfjvfseaum0) :time before 5m0s`, isErr: true},
		{spec: `device near polygon(c5vj26evvhfjvfseaum0) :time after`, isErr: true},
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
		{spec: `time gt 12: and time lt 15:00`, isErr: true},
		{spec: `datetime gte 2012-11-01T22:08:41+00:00 and datetime lt 2012-11-01T22:08:41+00:00`, isErr: true},
		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] { :trigger every hhh }`, isErr: true},
		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] { :trigger every 300s somelit }`, isErr: true},
		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] { :trigger 0x0 times }`, isErr: true},
		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] { :trigger 4 somelit }`, isErr: true},
		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] { :trigger 4 times some }`, isErr: true},
		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] { :trigger 4 times interval h4 }`, isErr: true},
		{spec: `
             device :radius 300m intersects line(c5vj26evvhfjvfseaum0) 
             and speed range [30 .. 120] { :trigger 4 times interval 300s somelit }`, isErr: true},
	}
	for _, tc := range testCases {
		expr, err := ParseSpec(tc.spec)
		if err != nil {
			if tc.isErr {
				continue
			} else {
				t.Fatal(err)
			}
		} else if tc.isErr {
			t.Fatalf("ParseSpec(%s) => nil, want err", tc.spec)
		}
		if expr == nil {
			t.Fatalf("ParseSpec(%s) => nil, want Expr", tc.spec)
		}
	}
}
