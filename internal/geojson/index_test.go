package geojson

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/tidwall/geojson"

	"github.com/uber/h3-go"

	"github.com/tidwall/geojson/geometry"

	"github.com/stretchr/testify/assert"
)

func TestIndex(t *testing.T) {
	data, err := loadData("./testdata/feature_collection_1.json")
	assert.NoError(t, err)
	o, err := geojson.Parse(data, geojson.DefaultParseOptions)
	assert.NoError(t, err)
	cells := EnsureIndex(o, 5)
	printCells(cells)
	// TODO:

}

func loadData(filename string) (string, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func printCell(cell h3.H3Index) {
	b := h3.ToGeoBoundary(cell)
	for _, b := range b {
		fmt.Println(b.Latitude, ",", b.Longitude)
	}
}

func printCells(cells []h3.H3Index) {
	for i := 0; i < len(cells); i++ {
		b := h3.ToGeoBoundary(cells[i])
		for _, b := range b {
			fmt.Println(b.Longitude, ",", b.Latitude)
		}
	}
}

func printRect(r geometry.Rect) {
	for i := 0; i < r.NumPoints(); i++ {
		fmt.Println(r.PointAt(i).X, ",", r.PointAt(i).Y)
	}
}
