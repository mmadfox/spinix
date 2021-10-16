package spinix

//import (
//	"context"
//	"math/rand"
//	"sync"
//	"testing"
//
//	"github.com/mmcloughlin/spherand"
//
//	"github.com/tidwall/geojson/geometry"
//
//	"github.com/rs/xid"
//)
//
//func TestRulesInsertDeleteConcurrent(t *testing.T) {
//	rules := NewRules()
//	var wait sync.WaitGroup
//	for i := 0; i < 100; i++ {
//		wait.Add(1)
//		go func() {
//			defer wait.Done()
//			ctx := context.Background()
//			for x := 0; x < 100; x++ {
//				lat, lon := spherand.Geographical()
//				meters := rand.Intn(1000000)
//				rule, err := NewRule("test", "{device.speed} >= 30", lat, lon, float64(meters))
//				if err != nil {
//					t.Fatal(err)
//				}
//				err = rules.Insert(ctx, rule)
//				if err != nil {
//					t.Fatal(err)
//				}
//				err = rules.Delete(ctx, rule.ID())
//				if err != nil {
//					t.Fatal(err)
//				}
//				rule, err = rules.FindOne(ctx, rule.ID())
//				if err == nil {
//					t.Fatalf("found rule, but expected not found")
//				}
//			}
//		}()
//	}
//	wait.Wait()
//}
//
//func TestRulesWalk(t *testing.T) {
//	rules := NewRules()
//	ctx := context.Background()
//	testCases := []struct {
//		radius   float64
//		lat, lon float64
//		level    int
//		objects  int
//		name     string
//	}{
//		{
//			// large region
//			name:    "rule1",
//			lat:     42.932296,
//			lon:     -72.2525488,
//			radius:  120000, // meters
//			level:   largeLevel,
//			objects: 1,
//		},
//		{
//			// small region
//			name:    "rule2",
//			lat:     42.9339633,
//			lon:     -72.3872273,
//			radius:  8000, // meters
//			level:   smallLevel,
//			objects: 2, // large + small
//		},
//		{
//			// small region
//			name:    "rule3",
//			lat:     42.7576506,
//			lon:     -72.0906276,
//			radius:  800, // meters
//			level:   smallLevel,
//			objects: 2, // large + small
//		},
//	}
//	for _, tc := range testCases {
//		circle, bbox := newCircle(tc.lat, tc.lon, tc.radius, getSteps(tc.radius))
//		regionLevel := getLevel(tc.radius)
//		regionIDs := cover(tc.radius, regionLevel, circle)
//		if err := rules.Insert(ctx, &Rule{
//			ruleID:      xid.New().String(),
//			bbox:        bbox,
//			name:        tc.name,
//			center:      geometry.Point{X: tc.lat, Y: tc.lon},
//			meters:      tc.radius,
//			regionLevel: regionLevel,
//			regionIDs:   regionIDs,
//		}); err != nil {
//			t.Fatal(err)
//		}
//	}
//	for _, tc := range testCases {
//		device := &Device{Latitude: tc.lat, Longitude: tc.lon}
//		var found int
//		if err := rules.Walk(ctx, device,
//			func(ctx context.Context, rule *Rule, err error) error {
//				found++
//				return nil
//			}); err != nil {
//			t.Fatal(err)
//		}
//		if found != tc.objects {
//			t.Fatalf("rules found %d, expected %d", found, tc.objects)
//		}
//	}
//}

//func BenchmarkRulesWalk(b *testing.B) {
//	ctx := context.Background()
//	rules := NewRules()
//	max := 100000
//	items := make([]*Rule, max)
//	for i := 0; i < max; i++ {
//		lat, lon := spherand.Geographical()
//		meters := float64(rand.Intn(180000))
//		rule, err := NewRule("test", "{device.speed} >= 30", lat, lon, meters)
//		if err != nil {
//			b.Fatal(err)
//		}
//		items[i] = rule
//		if err := rules.Insert(ctx, rule); err != nil {
//			b.Fatal(err)
//		}
//	}
//	b.ResetTimer()
//	b.SetParallelism(0)
//	var scan int
//	for i := 0; i < 10; i++ {
//		r := items[i%max]
//		device := &Device{
//			Latitude:  r.center.X,
//			Longitude: r.center.Y,
//		}
//		err := rules.Walk(ctx, device, func(ctx context.Context, rule *Rule, err error) error {
//			scan++
//			return nil
//		})
//		if err != nil {
//			b.Fatal(err)
//		}
//		log.Println("total", len(items), scan, r.meters)
//	}
//}
