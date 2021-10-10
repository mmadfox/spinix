package spinix

//func TestParseAllRules(t *testing.T) {
//	testCases := []string{
//		`(device(@) outside polygon(@poly1) OR device(@) intersects line(@line1))`,
//		`{device.speed} + 120 >= 300`,
//		`({device.speed} - 80 > 20) OR ({device.humidity} > 500)`,
//		`({device.speed} * 2 > 220) OR ({device.humidity} % 500 == 0)`,
//		`device(@) nearby polygon(@poly1) on distance 200`,
//		`device(@) nearby line(@line) on distance 500`,
//		`device nearby polygon(@id) on distance 100`,
//	}
//	for _, spec := range testCases {
//		expr, err := ParseSpec(spec)
//		if err != nil {
//			t.Fatal(err)
//		}
//		log.Println(expr)
//	}
//}
