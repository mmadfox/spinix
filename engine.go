package spinix

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/xid"
)

type Detector interface {
	Detect(ctx context.Context, device *Device) ([]Event, error)
}

type Option func(*Engine)

type Engine struct {
	rules      Rules
	objects    Objects
	geospatial Geospatial
}

func New(opts ...Option) *Engine {
	e := &Engine{
		rules:      NewRules(),
		objects:    nil,
		geospatial: DefaultGeospatial(),
	}
	for _, f := range opts {
		f(e)
	}
	return e
}

func WithObjects(o Objects) Option {
	return func(e *Engine) {
		e.objects = o
	}
}

func WithRules(r Rules) Option {
	return func(e *Engine) {
		e.rules = r
	}
}

func WithGeospatial(g Geospatial) Option {
	return func(e *Engine) {
		e.geospatial = g
	}
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

func (e *Engine) Map() Objects {
	return e.objects
}

func (e *Engine) Detect(ctx context.Context, device *Device) ([]Event, error) {
	events := make([]Event, 0, 4)
	if err := e.rules.Walk(ctx, device,
		func(ctx context.Context, rule *Rule, err error) error {
			if err != nil {
				return err
			}
			expr, err := eval(ctx, rule.expr, device, nil, e.geospatial, e.objects)
			if err != nil {
				return err
			}
			switch n := expr.(type) {
			case *BooleanLit:
				if !n.Value {
					return nil
				}
				event := Event{
					ID:       xid.New().String(),
					Device:   *device,
					DateTime: time.Now().Unix(),
				}
				event.Rule.ID = rule.ruleID
				event.Rule.Spec = rule.spec
				events = append(events, event)
			default:
				return fmt.Errorf("georule: unexpected result of the root expression: %#v", expr)
			}
			return nil
		}); err != nil {
		return nil, err
	}
	return events, nil
}

func (e *Engine) FindRule(ctx context.Context, ruleID string) (*Rule, error) {
	return e.rules.FindOne(ctx, ruleID)
}

func (e *Engine) HasRule(ctx context.Context, ruleID string) bool {
	rule, err := e.rules.FindOne(ctx, ruleID)
	if err == nil && rule != nil {
		return true
	}
	return false
}

func (e *Engine) RemoveRule(ctx context.Context, ruleID string) error {
	return e.rules.Delete(ctx, ruleID)
}

func (e *Engine) InsertRule(ctx context.Context, rule *Rule) error {
	if err := rule.validate(); err != nil {
		return err
	}
	refIDs := getRefVars(rule.expr)
	for _, rid := range refIDs {
		object, err := e.objects.Lookup(ctx, rid)
		if err != nil {
			return err
		}
		if !rule.boundingBox.ContainsRect(object.Rect()) {
			return fmt.Errorf("georule: the radius of the rule %.2f does not cover the object %s",
				rule.meters, rid)
		}
	}
	if err := e.rules.Insert(ctx, rule); err != nil {
		return err
	}
	return nil
}

func getRefVars(expr Expr) []string {
	vars := make([]string, 0, 2)
	WalkFunc(expr, func(expr Expr) {
		switch typ := expr.(type) {
		case *CallExpr:
			if typ.Fun.IsSpatialKeyword() {
				for _, arg := range typ.Args {
					lit, ok := arg.(*StringLit)
					if !ok {
						continue
					}
					vars = append(vars, lit.Value[1:])
				}
			}
		}
	})
	return vars
}
