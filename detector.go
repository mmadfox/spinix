package georule

import "context"

type Detector interface {
	Detect(context.Context, *Device) ([]Event, error)
}

type Event struct {
}
