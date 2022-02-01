package geojson

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/geojson"

	"github.com/uber/h3-go"
)

func TestEnsureIndex(t *testing.T) {
	testCases := []struct {
		filename string
		cells    int
		level    int
	}{
		{
			filename: "feature_collection_1",
			cells:    44,
			level:    6,
		},
		{
			filename: "feature_collection_2",
			cells:    2,
			level:    6,
		},
		{
			filename: "feature_collection_3",
			cells:    11,
			level:    2,
		},
		{
			filename: "feature_collection_4",
			cells:    4,
			level:    5,
		},
		{
			filename: "point_1",
			cells:    1,
			level:    6,
		},
		{
			filename: "multi_point_1",
			cells:    2,
			level:    6,
		},
		{
			filename: "line_string_1",
			cells:    11,
			level:    4,
		},
		{
			filename: "multi_line_string_1",
			cells:    1,
			level:    6,
		},
		{
			filename: "polygon_1",
			cells:    2,
			level:    2,
		},
		{
			filename: "empty_polygon",
			cells:    0,
			level:    6,
		},
		{
			filename: "multi_polygon_1",
			cells:    4,
			level:    2,
		},
		{
			filename: "feature_1",
			cells:    1,
			level:    5,
		},
	}
	parseOpts := geojson.DefaultParseOptions
	parseOpts.RequireValid = false
	for _, tc := range testCases {
		data, err := loadData("./testdata/" + tc.filename + ".json")
		assert.NoError(t, err)
		object, err := geojson.Parse(data, parseOpts)
		if err != nil {
			if !strings.Contains(err.Error(), "missing coordinates") {
				assert.NoError(t, err)
			}
		}

		cells := EnsureIndex(object, tc.level)
		assert.Equal(t, tc.cells, len(cells))

		if !testing.Short() {
			fmt.Printf("dataset: %s\n", tc.filename)
			printCells(cells)
		}
	}
}

func loadData(filename string) (string, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func printCells(cells []h3.H3Index) {
	for i := 0; i < len(cells); i++ {
		b := h3.ToGeoBoundary(cells[i])
		for _, b := range b {
			fmt.Println(b.Longitude, ",", b.Latitude)
		}
	}
}
