package spinix

import (
	"context"
	"testing"

	"github.com/mmadfox/geojson/geometry"
)

func TestObjectsNear(t *testing.T) {
	objects := NewMemoryObjects()
	ctx := context.Background()
	route := []geometry.Point{
		{X: 42.9283436, Y: -72.2757292},
		{X: 42.9286971, Y: -72.2767643},
		{X: 48.6619428, Y: -72.2784912},
	}
	o1 := polyFromString(`
-72.2783142, 42.9285754
-72.2781265, 42.9280922
-72.2771344, 42.9281904
-72.2772041, 42.9288385
-72.2783142, 42.9285793
-72.2783142, 42.9285754
`)
	o2 := polyFromString(`
-72.2795102, 42.9284065
-72.2792367, 42.9279783
-72.2782391, 42.9280805
-72.2788452, 42.9286029
-72.2795048, 42.9284261
-72.2795102, 42.9284065
`)
	if err := objects.Add(ctx, NewGeoObjectWithID(DefaultLayer, o1)); err != nil {
		t.Fatal(err)
	}
	if err := objects.Add(ctx, NewGeoObjectWithID(DefaultLayer, o2)); err != nil {
		t.Fatal(err)
	}
	var found int
	for _, point := range route {
		if err := objects.Near(ctx, DefaultLayer, point.X, point.Y, 500,
			func(ctx context.Context, o *GeoObject) error {
				found++
				return nil
			}); err != nil {
			t.Fatal(err)
		}
	}
	want := 4
	if found != want {
		t.Fatalf("have %d, want %d found objects", found, want)
	}
}
