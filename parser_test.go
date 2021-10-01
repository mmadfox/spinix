package georule

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	testCases := []struct {
		name  string
		rule  string
		isErr bool
		typ   Expr
	}{
		// success cases
		{
			name: "parse intersectsLine rule",
			rule: `(
                        intersectsLine(@line) AND intersectsLine(@lin2, @line1, @line3)
                   ) OR (
                        insidePolygon(@polygon1) and outsidePolygon(@polygon2)
                   )`,
			isErr: false,
			typ:   &BinaryExpr{},
		},

		{
			name: "parse insidePolygon rule",
			rule: `(
                        intersectsLine(@line) AND intersectsLine(@lin2, @line1, @line3)
                   ) OR (
                        insidePolygon(@polygon1) and outsidePolygon(@polygon2)
                   )`,
			isErr: false,
			typ:   &BinaryExpr{},
		},

		{
			name:  "parse speed rule",
			rule:  "speed(0, 20) OR speed(20)",
			isErr: false,
			typ:   &BinaryExpr{},
		},

		// failure cases
		{
			name:  "parse invalid someFunc rule",
			rule:  `someFunc(@line)`,
			isErr: true,
		},

		{
			name:  "parse to long ident",
			rule:  fmt.Sprintf("intersectsLine(@%s)", strings.Repeat("s", 257)),
			isErr: true,
		},

		{
			name:  "parse exceeds the number of arguments",
			rule:  "speed(0, 20, 30)",
			isErr: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := ParseString(tc.rule)
			if tc.isErr {
				if err == nil {
					t.Fatalf("ParseString(%s) => got nil, expected non nil error", tc.rule)
				} else {
					return
				}
			}
			if expr == nil {
				t.Fatalf("ParseString(%s) => got expr nil, expected non nil expr", tc.rule)
			} else {
				have := reflect.TypeOf(expr).Elem().Name()
				want := reflect.TypeOf(tc.typ).Elem().Name()
				if have != want {
					t.Fatalf("ParseString(%s) => got %s, expected %s", tc.rule, have, want)
				}
			}
		})
	}
}
