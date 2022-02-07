package geojson

import (
	"io/ioutil"
	"testing"

	h3geodist "github.com/mmadfox/go-h3geo-dist"
	"github.com/uber/h3-go/v3"

	"github.com/mmadfox/geojson"
	"github.com/stretchr/testify/assert"
)

func TestEnsureIndex(t *testing.T) {
	testCases := []struct {
		filename string
	}{
		{
			filename: "feature_collection_1",
		},
		{
			filename: "geometry_collection_1",
		},
		{
			filename: "feature_collection_1",
		},
		{
			filename: "feature_collection_2",
		},
		{
			filename: "feature_collection_3",
		},
		{
			filename: "feature_collection_5",
		},
		{
			filename: "point_1",
		},
		{
			filename: "multi_point_1",
		},
		{
			filename: "line_string_1",
		},
		{
			filename: "multi_line_string_1",
		},
		{
			filename: "polygon_1",
		},
		{
			filename: "multi_polygon_1",
		},
		{
			filename: "feature_1",
		},
	}

	parseOpts := geojson.DefaultParseOptions
	hosts := []string{
		"127.0.0.1",
		"127.0.0.2",
		"127.0.0.3",
		"127.0.0.4",
		"127.0.0.5",
	}
	stats := make(map[string]int)
	level := 3
	for _, tc := range testCases {
		data, err := loadData("./testdata/" + tc.filename + ".json")
		assert.NoError(t, err)
		object, err := geojson.Parse(data, parseOpts)
		assert.NoError(t, err)

		dist, _ := h3geodist.New(level)
		for _, host := range hosts {
			_ = dist.Add(host)
		}

		index, err := EnsureIndex(object, dist, level)
		assert.NoError(t, err)

		_ = index.ForEachHost(func(addr string, cells []h3.H3Index) error {
			if len(cells) == 0 {
				t.Fatalf("%s: covered 0, expected > 0 indexes", tc.filename)
			}
			stats[addr] += len(cells)
			return nil
		})
	}
	for _, host := range hosts {
		cnt, ok := stats[host]
		if !ok || cnt == 0 {
			t.Fatalf("%s: got 0, expected > 0", host)
		}
		t.Logf("%s: %d", host, cnt)
	}
}

func loadData(filename string) (string, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
