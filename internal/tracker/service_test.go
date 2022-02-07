package tracker

//
//func TestTracker_AddGeoJSON(t *testing.T) {
//	cluster := newLocalCluster("127.0.0.1", 3)
//	populateCluster(cluster, 12)
//	service := NewService(cluster)
//	ctx := context.Background()
//	object := loadGeoJSONFromFixture(t, 100, "geom1")
//	index, err := service.AddGeoJSON(ctx, object)
//	_ = index
//	_ = err
//}
//
//func populateCluster(c *localCluster, n int) {
//	if n < 1 {
//		n = 1
//	}
//	for i := 1; i < n; i++ {
//		c.AddHost(fmt.Sprintf("127.0.0.%d", i))
//	}
//}
//
//func loadGeoJSONFromFixture(t *testing.T, id uint64, filename string) GeoJSON {
//	data, err := ioutil.ReadFile("./testdata/" + filename + ".json")
//	if err != nil {
//		t.Fatal(err)
//	}
//	object, err := geojson.Parse(string(data), geojson.DefaultParseOptions)
//	if err != nil {
//		t.Fatal(err)
//	}
//	return GeoJSON{
//		ObjectID: id,
//		LayerID:  id + 100,
//		Data:     object,
//	}
//}
