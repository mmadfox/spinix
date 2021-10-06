package georule

import (
	"context"
	"encoding/json"
	"fmt"
)

type Detector interface {
	Detect(ctx context.Context, target *Device, state *State) ([]Event, error)
	AddRule(ctx context.Context, rule Rule) error
	DeleteRule(ctx context.Context, ruleID string) error
}

const MaxSpecLen = 2048

type Rule struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Spec      string     `json:"spec"`
	Variables []Variable `json:"vars"`
}

func (r Rule) String() string {
	return fmt.Sprintf("Rule{ID:%s, Name:%s, Spec:%s}", r.ID, r.Name, r.Spec)
}

func (r Rule) Validate() error {
	if len(r.ID) == 0 {
		return fmt.Errorf("georule: rule without id")
	}
	if len(r.Spec) == 0 {
		return fmt.Errorf("georule: rule without spec")
	}
	if len(r.Spec) > MaxSpecLen {
		return fmt.Errorf("georule: rule spec too long")
	}
	return nil
}

type Variable struct {
	ID    string      `json:"id"`
	Value interface{} `json:"value"`
}

type Event struct {
	ID       string `json:"id"`
	Device   Device `json:"device"`
	DateTime int64  `json:"dateTime"`
	Rule     struct {
		ID   string `json:"ruleId"`
		Spec string `json:"spec"`
	} `json:"rule"`
}

func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(e)
}

func (e *Event) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, e); err != nil {
		return err
	}
	return nil
}
