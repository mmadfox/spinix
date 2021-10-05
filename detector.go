package georule

import "context"

type Detector interface {
	Detect(context.Context, *Device) ([]Event, error)
}

type Event struct {
	ID       string `json:"id"`
	Device   Device `json:"device"`
	DateTime int64  `json:"dateTime"`
	Rule     struct {
		Spec     string   `json:"spec"`
		Triggers []string `json:"triggers"`
	} `json:"rule"`
}
