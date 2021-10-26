package spinix

import (
	"fmt"
	"testing"
)

func TestNewRule(t *testing.T) {
	rule, err := NewRule(`device intersects polygon(@poly) { :center 42.3341249 -72.236952 :radius 139km }`)
	if err != nil {
		t.Fatal(err)
	}
	regions := rule.Regions()
	for _, reg := range regions {
		for _, p := range reg.Bounding() {
			fmt.Println(p.Y, ",", p.X)
		}
	}
	for i := 0; i < rule.Circle().NumPoints(); i++ {
		p := rule.Circle().PointAt(i)
		fmt.Println(p.Y, ",", p.X)
	}
}
