package georule

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/tidwall/geojson/geometry"

	"github.com/tidwall/geojson"
)

const MaxSpecLen = 2048

type Detector interface {
	Detect(ctx context.Context, target *Device, state *State) ([]Event, error)
	AddRule(ctx context.Context, rule Rule) error
	DeleteRule(ctx context.Context, ruleID string) error
}

type Rule struct {
	ID        string     `json:"ruleId"`
	Name      string     `json:"name"`
	Spec      string     `json:"spec"`
	Variables []Variable `json:"vars"`
}

func EncodeRuleToJSON(r Rule) ([]byte, error) {
	buf := &bytes.Buffer{}
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(r)
	return buf.Bytes(), err
}

func DecodeRuleFromJSON(data []byte) (Rule, error) {
	var r Rule
	if err := json.Unmarshal(data, &r); err != nil {
		return Rule{}, err
	}
	return r, nil
}

func (r Rule) String() string {
	return fmt.Sprintf("Rule{Name:%s, Name:%s, ParseSpec:%s}", r.ID, r.Name, r.Spec)
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
	Name   string  `json:"name"`
	Object GeoJSON `json:"object"`
}

type GeoJSON struct {
	geojson.Object
}

func (o *GeoJSON) UnmarshalJSON(data []byte) error {
	object, err := geojson.Parse(string(data), &geojson.ParseOptions{
		IndexChildren:     64,
		IndexGeometry:     64,
		IndexGeometryKind: geometry.QuadTree,
		RequireValid:      false,
		AllowSimplePoints: true,
		DisableCircleType: false,
		AllowRects:        true,
	})
	if err != nil {
		return err
	}
	o.Object = object
	return nil
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
