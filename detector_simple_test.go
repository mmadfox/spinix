package georule

import (
	"context"
	"testing"
)

func TestSimpleDetector(t *testing.T) {
	detector := NewSimpleDetector()
	ctx := context.Background()
	err := detector.AddRule(ctx, Rule{ID: "1", Name: "The rule for speed 0-5 km/h", Spec: "{device.speed} >= 0 OR {device.speed} < 5"})
	if err != nil {
		t.Fatal(err)
	}
	err = detector.AddRule(ctx, Rule{ID: "2", Name: "The rule for speed 10-25 km/h", Spec: "{device.speed} >= 10 OR {device.speed} < 25"})
	if err != nil {
		t.Fatal(err)
	}
	myDevice := &Device{
		IMEI: "myDevice",
	}
	state := NewState(myDevice.IMEI)
	for i := 0; i < 3; i++ {
		events, err := detector.Detect(ctx, myDevice, state)
		if err != nil {
			t.Fatal(err)
		}
		myDevice.Speed += 3
		if have, want := len(events), 1; have < want {
			t.Fatalf("no events found")
		}
	}
}
