package spinix

import (
	"context"
	"fmt"
	"time"

	"github.com/mmadfox/geojson/geometry"

	"github.com/rs/xid"
)

type Detector interface {
	Detect(ctx context.Context, device *Device) ([]Event, bool, error)
}

type Option func(*Engine)

type Engine struct {
	refs reference

	beforeDetect []BeforeDetectFunc
	afterDetect  []AfterDetectFunc
}

func New(opts ...Option) *Engine {
	e := &Engine{
		refs:         defaultRefs(),
		beforeDetect: []BeforeDetectFunc{},
		afterDetect:  []AfterDetectFunc{},
	}
	for _, f := range opts {
		f(e)
	}
	return e
}

func WithDetectBefore(fn ...BeforeDetectFunc) Option {
	return func(e *Engine) {
		e.beforeDetect = append(e.beforeDetect, fn...)
	}
}

func WithDetectAfter(fn ...AfterDetectFunc) Option {
	return func(e *Engine) {
		e.afterDetect = append(e.afterDetect, fn...)
	}
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
	ID       string       `json:"ID"`
	Device   Device       `json:"device"`
	DateTime int64        `json:"dateTime"`
	Rule     RuleSnapshot `json:"rule"`
	Match    []Match      `json:"match"`
}

func MakeEvent(d *Device, r *Rule, m []Match) Event {
	event := Event{
		ID:       xid.New().String(),
		Device:   *d,
		Rule:     r.Snapshot(),
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

func (e *Engine) AssignCoordsFromSpec(ctx context.Context, rule *Rule) (err error) {
	if err = rule.spec.validate(); err != nil {
		if err = e.calcCenter(ctx, rule); err != nil {
			return err
		}
	} else {
		if err = e.expand(ctx, rule); err != nil {
			return err
		}
	}
	return
}

func (e *Engine) AddRule(ctx context.Context, spec string) (*Rule, error) {
	rule, err := NewRule(spec)
	if err != nil {
		return nil, err
	}
	if err := e.AssignCoordsFromSpec(ctx, rule); err != nil {
		return nil, err
	}
	if err := e.refs.rules.Insert(ctx, rule); err != nil {
		return nil, err
	}
	return rule, nil
}

func (e *Engine) calcCenter(ctx context.Context, rule *Rule) error {
	refs := rule.RefIDs()
	var bbox geometry.Rect
	for refID, tok := range refs {
		if !isObjectToken(tok) || tok == DEVICES {
			continue
		}
		object, err := e.refs.objects.Lookup(ctx, refID)
		if err != nil {
			return fmt.Errorf("%w - failed to add rule [%s]", err, rule.specStr)
		}
		bbox = e.calcBounding(bbox, object.data.Rect())
	}
	rule.spec.props.center = bbox.Center()
	return rule.calc()
}

func (e *Engine) expand(ctx context.Context, rule *Rule) error {
	if err := rule.spec.validate(); err != nil {
		return err
	}
	refs := rule.RefIDs()
	var ok bool
	if refs != nil {
		for i := 0; i < 10; i++ {
			var bbox geometry.Rect
			circle := circleFromRule(rule)
			for refID, tok := range refs {
				if !isObjectToken(tok) || tok == DEVICES {
					continue
				}
				object, err := e.refs.objects.Lookup(ctx, refID)
				if err != nil {
					return fmt.Errorf("%w - failed to add rule [%s]", err, rule.specStr)
				}
				bbox = e.calcBounding(bbox, object.data.Rect())
			}
			if circle.ContainsRect(bbox) {
				ok = true
				break
			}
			rule.spec.props.radius *= 3
			if err := rule.calc(); err != nil {
				return err
			}
		}
		if !ok {
			return fmt.Errorf("spinix/engine: rule does not contains any objects")
		}
	}
	return nil
}

func (e *Engine) Detect(ctx context.Context, device *Device) (events []Event, ok bool, err error) {
	device.DetectRegion()
	err = e.refs.rules.Walk(ctx, device.Latitude, device.Longitude,
		func(ctx context.Context, rule *Rule, err error) error {
			if err != nil {
				return err
			}
			for _, beforeFunc := range e.beforeDetect {
				if ok := beforeFunc(device, rule); ok {
					continue
				}
			}
			match, status, err := rule.spec.evaluate(ctx, rule.id, device, e.refs)
			if err != nil {
				return err
			}
			if status {
				ok = true
				if events == nil {
					events = make([]Event, 0, 2)
				}
				events = append(events, MakeEvent(device, rule, match))
			}
			for _, afterFunc := range e.afterDetect {
				afterFunc(device, rule, ok, events)
			}
			return nil
		})
	if err == nil {
		if _, err = e.refs.devices.InsertOrReplace(ctx, device); err != nil {
			return nil, false, err
		}
	}
	device.ResetRegion()
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
