package spinix

import (
	"context"
	"testing"
)

var poly = polyFromString(`
-72.4276075, 43.8662180
-72.4276075, 43.9295499
-73.5699012, 42.9081601
-71.8125263, 42.9081601
-72.4276075, 43.8662180
`)

func TestRuleMarshalUnmarshalJSON(t *testing.T) {
	wantSpec := `
       device :radius 5km near devices(@imei1, @imei2, @imei3) 
       OR datetime range ["2012-10-01T22:08:41+00:00" .. "2012-11-01T22:08:41+00:00"]
       { 
          :center 43.9295499 -72.4276075 
          :reset after 3h
          :trigger every 20m
       }`

	rule, err := NewRule(wantSpec)
	if err != nil {
		t.Fatal(err)
	}
	data, err := rule.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	rule2 := new(Rule)
	if err := rule2.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	if have, want := rule.Specification(), rule2.Specification(); have != want {
		t.Fatalf("have %s, want %s specification", have, want)
	}
	if have, want := rule.ID(), rule2.ID(); have != want {
		t.Fatalf("have %s, want %s rule id", have, want)
	}
	if have, want := len(rule.RegionIDs()), len(rule2.RegionIDs()); want != have {
		t.Fatalf("have %device, want %device RegionIDs", have, want)
	}
	r1regionIDs := rule.RegionIDs()
	r2regionIDs := rule2.RegionIDs()
	for i := 0; i < len(r1regionIDs); i++ {
		want := r1regionIDs[i]
		have := r2regionIDs[i]
		if have != want {
			t.Fatalf("have %device, want %device", have, want)
		}
	}
	if have, want := rule.RegionSize(), rule2.RegionSize(); want != have {
		t.Fatalf("have %device, want %device regionSize", have, want)
	}

	// fails
	if err := rule2.UnmarshalJSON([]byte(`{}`)); err == nil {
		t.Fatalf("have nil, want error")
	}
	if err := rule2.UnmarshalJSON([]byte(`{"spec":"device near device"}`)); err == nil {
		t.Fatalf("have nil, want error")
	}
}

func TestRulesInsert(t *testing.T) {
	ctx := context.Background()
	refs := defaultRefs()
	_ = refs.objects.Add(ctx, NewGeoObject("poly", DefaultLayer, poly))
	rulesInMem := NewMemoryRules()
	testCases := []struct {
		spec     string
		lat, lon float64
		err      bool
	}{
		{
			spec: `device intersects polygon(@poly) { :center 42.3341249 -72.236952 :radius 139km }`,
			lat:  41.6072282,
			lon:  -71.3731825,
		},
		{
			spec: `device intersects polygon(@poly) { :center 42.3324467 -72.2679364 :radius 1000m }`,
			lat:  44.1191415,
			lon:  -73.0426887,
		},
	}
	for _, tc := range testCases {
		rule, err := NewRule(tc.spec)
		if err != nil {
			if tc.err {
				continue
			} else {
				t.Fatalf("NewRule(%s) => error %v, want nil", tc.spec, err)
			}
		} else if tc.err {
			t.Fatalf("NewRule(%s) => nil, want error", tc.spec)
		}
		if err := rulesInMem.Insert(ctx, rule); err != nil {
			t.Fatal(err)
		}
	}
	var rules int
	err := rulesInMem.Walk(ctx, &Device{Latitude: 42.3341249, Longitude: -72.236952},
		func(ctx context.Context, rule *Rule, err error) error {
			rules++
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if rules != 1 {
		t.Fatalf("have %device, want 1", rules)
	}
}
