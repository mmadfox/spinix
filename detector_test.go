package georule

import (
	"strings"
	"testing"
)

func TestRuleMarshalUnmarshal(t *testing.T) {
	testCases := []struct {
		name  string
		raw   string
		isErr bool
	}{
		{
			name: "Rule one",
			raw:  `{"ruleId":"one","name":"My Rule","spec":"{device.speed} >= 10 OR {device.speed} < 25}","vars":[{"name":"myVar","object":{"type":"Polygon","coordinates":[[[100,0],[101,0],[101,1],[100,1],[100,0]],[[100.2,0.2],[100.8,0.2],[100.8,0.8],[100.2,0.8],[100.2,0.2]]]}}]}`,
		},
	}
	for _, tc := range testCases {
		r, err := DecodeRuleFromJSON([]byte(tc.raw))
		if err != nil && !tc.isErr {
			t.Fatal(err)
		}
		data, err := EncodeRuleToJSON(r)
		if err != nil {
			t.Fatal(err)
		}
		a := strings.TrimSpace(string(data))
		b := strings.TrimSpace(tc.raw)
		if a != b {
			t.Fatalf("%s => rules not equal %s != %s", tc.name, a, b)
		}
	}
}
