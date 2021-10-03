package georule

import "testing"

func TestFromString(t *testing.T) {
	testCases := []struct {
		name  string
		spec  string
		isErr bool
	}{
		{
			name: "spec1",
			spec: "({device.status} == 1 OR {device.status} IN [2,4]) OR ({device.status} >= 0 AND {device.status} < 10)",
		},
		{
			name: "spec2",
			spec: "({device.status} == 1 OR {device.status} IN [2,4]) OR ({device.status} >= 0 AND {device.status} < 10)",
		},
		{
			name:  "spec3",
			spec:  "",
			isErr: true,
		},
		{
			name:  "spec4",
			spec:  "badFunc()",
			isErr: true,
		},
	}
	for _, tc := range testCases {
		spec, err := Spec(tc.name, tc.spec)
		if tc.isErr {
			if err == nil {
				t.Fatalf("Spec(%s, %s) => got nil, expected non nil error", tc.name, tc.spec)
			} else {
				return
			}
		}
		if spec.ID().IsNil() {
			t.Fatal("specification id is nil")
		}
		if have, want := spec.String(), tc.spec; have != want {
			t.Fatalf("have %s, want %s", have, want)
		}
		if have, want := spec.Name(), tc.name; have != want {
			t.Fatalf("have %s, want %s", have, want)
		}
	}
}
