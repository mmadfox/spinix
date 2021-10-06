package georule

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/rs/xid"
)

var _ Detector = &SimpleDetector{}

type SimpleDetectorOption func(*SimpleDetector)

type SimpleDetector struct {
	mu         sync.RWMutex
	rules      map[string]S
	vars       Vars
	geospatial Geospatial
}

func NewSimpleDetector() *SimpleDetector {
	return &SimpleDetector{
		rules:      make(map[string]S),
		vars:       NewInMemVars(),
		geospatial: geospatial{},
	}
}

func WithSimpleDetectorVars(v Vars) SimpleDetectorOption {
	return func(d *SimpleDetector) {
		d.vars = v
	}
}

func WithSimpleDetectorGeospatial(g Geospatial) SimpleDetectorOption {
	return func(d *SimpleDetector) {
		d.geospatial = g
	}
}

func (d *SimpleDetector) Detect(ctx context.Context, device *Device, state *State) (events []Event, err error) {
	if device == nil || state == nil {
		return nil, fmt.Errorf("georule/detect: no device or state")
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	events = make([]Event, 0, 8)
	for id, spec := range d.rules {
		res, err := eval(ctx, spec.Expr(), device, state, d.geospatial, d.vars)
		if err != nil {
			return nil, err
		}
		switch n := res.(type) {
		case *BooleanLit:
			if n.Value {
				event := Event{
					ID:       xid.New().String(),
					Device:   *device,
					DateTime: time.Now().Unix(),
				}
				event.Rule.ID = id
				event.Rule.Spec = spec.String()
				events = append(events, event)
			}
		default:
			return nil, fmt.Errorf("georule/detect: unexpected result of the root expression: %#v", res)
		}
	}
	state.Update(device)
	return
}

func (d *SimpleDetector) AddRule(ctx context.Context, rule Rule) error {
	if err := rule.Validate(); err != nil {
		return err
	}

	spec, err := Spec(rule.ID, rule.Name, rule.Spec)
	if err != nil {
		return err
	}

	d.mu.Lock()
	_, found := d.rules[rule.ID]
	if found {
		d.mu.Unlock()
		return fmt.Errorf("georule/detect: rule %s already exists", rule)
	}
	d.rules[rule.ID] = spec
	d.mu.Unlock()

	if len(rule.Variables) > 0 {
		vars := VarsFromSpec(spec)
		for _, v := range rule.Variables {
			_, found := vars[v.ID]
			if !found {
				continue
			}
			if err := d.vars.Set(ctx, v.ID, v.Value); err != nil {
				if der := d.DeleteRule(ctx, rule.ID); der != nil {
					return multierror.Append(err, der)
				} else {
					return err
				}
			}
		}
	}
	return nil
}

func (d *SimpleDetector) DeleteRule(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.rules, id)
	return nil
}
