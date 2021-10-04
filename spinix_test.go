package georule

import "testing"

func TestDetect(t *testing.T) {
	state := NewState()
	testCases := []struct {
		name   string
		spec   S
		detect bool
		isErr  bool
		device *Device
	}{
		{
			name:   "detect.speed 1",
			spec:   makeRule(t, "1", "one", "{device.speed} >= 5"),
			detect: false,
			device: &Device{Speed: 1},
		},
		{
			name:   "detect.speed 10",
			spec:   makeRule(t, "1", "one", "{device.speed} >= 5"),
			detect: true,
			device: &Device{Speed: 10},
		},
	}
	for _, tc := range testCases {
		ok, err := Detect(tc.spec, tc.device, state)
		if err != nil {
			t.Fatal(err)
		}
		if tc.detect != ok {
			t.Fatalf("Detect(%s) => false, want true", tc.spec)
		}
		state.Update(tc.device)
	}
}

func makeRule(t *testing.T, id string, name string, rule string) S {
	s, err := Spec(id, name, rule)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
