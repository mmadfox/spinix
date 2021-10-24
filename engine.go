package spinix

import (
	"context"
	"fmt"
	"time"

	"github.com/tidwall/geojson/geometry"

	"github.com/tidwall/geojson"

	"github.com/rs/xid"
)

type Detector interface {
	Detect(ctx context.Context, device *Device) ([]Event, error)
}

type Option func(*Engine)

type Engine struct {
	refs reference
}

func New(opts ...Option) *Engine {
	e := &Engine{refs: defaultRefs()}
	for _, f := range opts {
		f(e)
	}
	return e
}

func WithObjectsStorage(o Objects) Option {
	return func(e *Engine) {
		e.refs.objects = o
	}
}

func WithDevicesStorage(d Devices) Option {
	return func(e *Engine) {
		e.refs.devices = d
	}
}

func WithRulesStorage(r Rules) Option {
	return func(e *Engine) {
		e.refs.rules = r
	}
}

func WithStatesStorage(s States) Option {
	return func(e *Engine) {
		e.refs.states = s
	}
}

type Event struct {
	ID       string       `json:"id"`
	Device   Device       `json:"device"`
	DateTime int64        `json:"dateTime"`
	Rule     RuleSnapshot `json:"rule"`
	Match    []Match      `json:"match"`
}

func MakeEvent(d *Device, r *Rule, m []Match) Event {
	event := Event{
		ID:       xid.New().String(),
		Device:   *d,
		Rule:     Snapshot(r),
		DateTime: time.Now().Unix(),
		Match:    make([]Match, len(m)),
	}
	copy(event.Match, m)
	return event
}

func (e *Engine) Objects() Objects {
	return e.refs.objects
}

func (e *Engine) Rules() Rules {
	return e.refs.rules
}

func (e *Engine) Devices() Devices {
	return e.refs.devices
}

func (e *Engine) States() States {
	return e.refs.states
}

func (e *Engine) AddObject(ctx context.Context, objectID string, object geojson.Object) error {
	if object == nil {
		return fmt.Errorf("spinix/engine: object %s is not defined", objectID)
	}
	return e.refs.objects.Add(ctx, objectID, object)
}

func (e *Engine) AddRule(ctx context.Context, name string, owner string, spec string, lat float64, lon float64, meters float64) (*Rule, error) {
	if meters <= 0 {
		meters = 3000
	}
	rule, err := NewRule(name, owner, spec, lat, lon, meters)
	if err != nil {
		return nil, err
	}
	refs := rule.RefIDs()
	var ok bool
	if refs != nil {
		for i := 0; i < 10; i++ {
			var bbox geometry.Rect
			circle := &geometry.Poly{Exterior: rule.Circle()}
			for refID, tok := range refs {
				if !isObjectToken(tok) || tok == DEVICES {
					continue
				}
				object, err := e.refs.objects.Lookup(ctx, refID)
				if err != nil {
					return nil, err
				}
				bbox = e.calcBounding(bbox, object.Rect())
			}
			if circle.ContainsRect(bbox) {
				ok = true
				break
			}
			rule.calc(rule.meters * 2)
		}
		if !ok {
			return nil, fmt.Errorf("spinix/engine: the radius of the rule does not cover geoobjects")
		}
	}
	if err := e.refs.rules.Insert(ctx, rule); err != nil {
		return nil, err
	}
	return rule, nil
}

func (e *Engine) Detect(ctx context.Context, device *Device) (events []Event, err error) {
	err = e.refs.rules.Walk(ctx, device,
		func(ctx context.Context, rule *Rule, err error) error {
			if err != nil {
				return err
			}
			match, ok, err := rule.spec.evaluate(ctx, rule.ruleID, device, e.refs)
			if err != nil {
				return err
			}
			if ok {
				if events == nil {
					events = make([]Event, 0, 2)
				}
				events = append(events, MakeEvent(device, rule, match))
			}
			return nil
		})
	if err == nil {
		if err := e.refs.devices.InsertOrReplace(ctx, device); err != nil {
			return nil, err
		}
	}
	return
}

func (e *Engine) calcBounding(a, b geometry.Rect) (bbox geometry.Rect) {
	if a.Min.X == 0 && a.Min.Y == 0 &&
		a.Max.X == 0 && a.Max.Y == 0 {
		return b
	}
	if b.Min.X < a.Min.X {
		bbox.Min.X = b.Min.X
	} else {
		bbox.Min.X = a.Min.X
	}
	if b.Max.X > a.Max.X {
		bbox.Max.X = b.Max.X
	} else {
		bbox.Max.X = a.Max.X
	}
	if b.Min.Y < a.Min.Y {
		bbox.Min.Y = b.Min.Y
	} else {
		bbox.Min.Y = a.Min.Y
	}
	if b.Max.Y > a.Max.Y {
		bbox.Max.Y = b.Max.Y
	} else {
		bbox.Max.Y = a.Max.Y
	}
	return
}
