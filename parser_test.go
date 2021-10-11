package spinix

import (
	"testing"
)

func TestParseSpecSuccess(t *testing.T) {
	testCases := []string{
		// geospatial nearby
		`device nearby polygon(@poly1) on distance 200`,
		`device nearby devices(@deviceID) on distance 300`,
		`device nearby polygon(@poly1, @poly2) on distance 200`,
		`(
             device nearby polygon(@poly1, @poly2) on distance 200
         ) OR (
             device nearby circle(@circle1) on distance 500 
         ) OR (
             device nearby devices(@three, @two) on distance 500 
         )`,

		// geospatial distance to
		`device distance to devices(@one, @two) range [300-1000]`,
		`device distance to collection(@col) >= 3000`,

		// geospatial intersects
		`device intersects devices(@one) on distance 400`,
		`device intersects collection(@col)`,
		`device intersects circle(@id) on distance 1500`,
		`(device intersects circle(@id) on distance 1500) AND ({device.speed} >= 5)`,
		`device intersects circle(@id) on distance 1500 AND {device.speed} >= 50`,

		// variables
		`{device.speed} + 120 >= 300`,
		`({device.speed} - 80 > 20) OR ({device.humidity} > 500)`,
		`({device.speed} * 2 > 220) OR ({device.humidity} % 500 == 0)`,
	}
	for _, spec := range testCases {
		expr, err := ParseSpec(spec)
		if err != nil {
			t.Fatal(err)
		}
		if expr == nil {
			t.Fatalf("ParseSpec(%s) => nil, want not nil", spec)
		}
	}
}

func TestParseSpecFailure(t *testing.T) {
	testCases := []string{
		// not defined
		"",
		"some text",
		"nearby nearby nearby polygon(@poly1) on distance 200",
		// geospatial nearby
		`nearby polygon(@poly1) on distance 200`,
		`nearby distance 300`,
	}
	for _, spec := range testCases {
		expr, err := ParseSpec(spec)
		if err == nil {
			t.Fatalf("ParseSpec(%s) => nil, want error", spec)
		}
		if expr != nil {
			t.Fatalf("ParseSpec(%s) => not nil, want nil", spec)
		}
	}
}
