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

func TestRulesInsert(t *testing.T) {
	ctx := context.Background()
	refs := defaultRefs()
	_ = refs.objects.Add(ctx, "poly", poly)
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
		t.Fatalf("have %d, want 1", rules)
	}
}
