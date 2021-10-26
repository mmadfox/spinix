package spinix

import (
	"strconv"
	"strings"

	"github.com/tidwall/geojson"

	"github.com/tidwall/geojson/geometry"
)

func pointsFromString(s string) []geometry.Point {
	lines := strings.Split(s, "\n")
	res := make([]geometry.Point, 0)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		points := strings.Split(line, ",")
		lats := strings.Trim(points[1], " ")
		lons := strings.Trim(points[0], " ")
		if len(lats) == 0 && len(lons) == 0 {
			continue
		}
		lat, err := strconv.ParseFloat(lats, 10)
		if err != nil {
			panic(err)
		}
		lon, err := strconv.ParseFloat(lons, 10)
		if err != nil {
			panic(err)
		}
		res = append(res, geometry.Point{
			X: lat,
			Y: lon,
		})
	}
	return res
}

func polyFromString(s string) *geojson.Polygon {
	res := pointsFromString(s)
	return geojson.NewPolygon(geometry.NewPoly(res, nil, nil))
}
