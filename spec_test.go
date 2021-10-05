package georule

import (
	"testing"
)

func TestVars(t *testing.T) {
	s, err := Spec("id", "test", "intersectsPoly(@id1, @id2, @id3) OR intersectsPoly(@id4, @id4, @id4)")
	if err != nil {
		t.Fatal(err)
	}
	vars := VarsFromSpec(s)
	if len(vars) != 4 {
		t.Fatalf("VarsFromSpec(%s) => %d, want 4", s, len(vars))
	}
}

func TestFromString(t *testing.T) {
	testCases := []struct {
		name  string
		id    string
		spec  string
		isErr bool
	}{
		{
			id:   "1",
			name: "spec1",
			spec: "({device.status} == 1 OR {device.status} IN [2,4]) OR ({device.status} >= 0 AND {device.status} < 10)",
		},
		{
			id:   "2",
			name: "spec2",
			spec: "({device.status} == 1 OR {device.status} IN [2,4]) OR ({device.status} >= 0 AND {device.status} < 10)",
		},
		{
			id:    "3",
			name:  "spec3",
			spec:  "",
			isErr: true,
		},
		{
			id:    "4",
			name:  "spec4",
			spec:  "badFunc()",
			isErr: true,
		},
	}
	for _, tc := range testCases {
		spec, err := Spec(tc.id, tc.name, tc.spec)
		if tc.isErr {
			if err == nil {
				t.Fatalf("Spec(%s, %s) => got nil, expected non nil error", tc.name, tc.spec)
			} else {
				return
			}
		}
		if have, want := spec.String(), tc.spec; have != want {
			t.Fatalf("have %s, want %s", have, want)
		}
		if have, want := spec.Name(), tc.name; have != want {
			t.Fatalf("have %s, want %s", have, want)
		}
		if have, want := spec.ID(), tc.id; have != want {
			t.Fatalf("have %s, want %s", have, want)
		}
	}
}
