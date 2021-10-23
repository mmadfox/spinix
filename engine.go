package spinix

import (
	"context"
	"time"

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
}

func MakeEvent(d *Device, r *Rule) Event {
	return Event{
		ID:       xid.New().String(),
		Device:   *d,
		Rule:     TakeRuleSnapshot(r),
		DateTime: time.Now().Unix(),
	}
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

func (e *Engine) Detect(ctx context.Context, device *Device) ([]Event, error) {
	return nil, nil
}

//func (e *Engine) Detect(ctx context.Context, device *Device) ([]Event, error) {
//	prevState, err := e.devices.Lookup(ctx, device.IMEI)
//	if err != nil {
//		err = nil
//		prevState = device
//	}
//	events := make([]Event, 0, 4)
//	if err := e.rules.Walk(ctx, device,
//		func(ctx context.Context, rule *Rule, err error) error {
//			if err != nil {
//				return err
//			}
//			nodes, err := e.invokeSpec(ctx, rule.nodes, prevState, device)
//			if err != nil {
//				return err
//			}
//			switch n := nodes.(type) {
//			case *BooleanLit:
//				if !n.Value {
//					return nil
//				}
//				events = append(events, MakeEvent(device, rule))
//			default:
//				return fmt.Errorf("georule: unexpected result of the root expression: %#v", nodes)
//			}
//			return nil
//		}); err != nil {
//		return nil, err
//	}
//	if err := e.devices.InsertOrReplace(ctx, device); err != nil {
//		return nil, err
//	}
//	return events, nil
//}
//
//func (e *Engine) FindRule(ctx context.Context, ruleID string) (*Rule, error) {
//	return e.rules.FindOne(ctx, ruleID)
//}
//
//func (e *Engine) HasRule(ctx context.Context, ruleID string) bool {
//	rule, err := e.rules.FindOne(ctx, ruleID)
//	if err == nil && rule != nil {
//		return true
//	}
//	return false
//}
//
//func (e *Engine) RemoveRule(ctx context.Context, ruleID string) error {
//	return e.rules.Delete(ctx, ruleID)
//}
//
//func (e *Engine) InsertRule(ctx context.Context, rule *Rule) error {
//	if err := rule.validate(); err != nil {
//		return err
//	}
//	refIDs := getRefVars(rule.nodes)
//	for _, rid := range refIDs {
//		object, err := e.objects.Lookup(ctx, rid)
//		if err != nil {
//			return err
//		}
//		if !rule.bbox.ContainsRect(object.Rect()) {
//			return fmt.Errorf("georule: the radius of the rule %.2f does not cover the object %s",
//				rule.meters, rid)
//		}
//	}
//	if err := e.rules.Insert(ctx, rule); err != nil {
//		return err
//	}
//	return nil
//}
//
//func (e *Engine) InvokeSpec(ctx context.Context, nodes Expr, device *Device) (Expr, error) {
//	return e.invokeSpec(ctx, nodes, device, device)
//}
//
//func (e *Engine) invokeSpec(ctx context.Context, nodes Expr, prevState, currentState *Device) (Expr, error) {
//	var (
//		err    error
//		lv, rv Expr
//	)
//
//	switch n := nodes.(type) {
//	case *ParenExpr:
//		return e.invokeSpec(ctx, nodes, prevState, currentState)
//	case *BinaryExpr:
//		lv, err = e.invokeSpec(ctx, n.LHS, prevState, currentState)
//		if err != nil {
//			return falseExpr, err
//		}
//		rv, err = e.invokeSpec(ctx, n.RHS, prevState, currentState)
//		if err != nil {
//			return falseExpr, err
//		}
//		return e.applyOperator(ctx, n.Op, lv, rv, prevState, currentState)
//	case *VarLit:
//		switch n.Value {
//		case VAR_SPEED:
//			return &FloatLit{Value: currentState.Speed}, nil
//		case VAR_BATTERY:
//			return &FloatLit{Value: currentState.BatteryCharge}, nil
//		case VAR_TEMPERATURE:
//			return &FloatLit{Value: currentState.Temperature}, nil
//		case VAR_HUMIDITY:
//			return &FloatLit{Value: currentState.Humidity}, nil
//		case VAR_LUMONOSITY:
//			return &FloatLit{Value: currentState.Luminosity}, nil
//		case VAR_PRESSURE:
//			return &FloatLit{Value: currentState.Pressure}, nil
//		case VAR_FUELLEVEL:
//			return &FloatLit{Value: currentState.FuelLevel}, nil
//		case VAR_MODEL:
//			return &StringLit{Value: currentState.Model}, nil
//		case VAR_BRAND:
//			return &StringLit{Value: currentState.Brand}, nil
//		case VAR_OWNER:
//			return &StringLit{Value: currentState.Owner}, nil
//		case VAR_EMEI:
//			return &StringLit{Value: currentState.IMEI}, nil
//		case VAR_STATUS:
//			return &IntLit{Value: currentState.Status}, nil
//		}
//	case *CallExpr:
//		_ = n
//	default:
//		_ = n
//	}
//	return nodes, nil
//}
//
//func getRefVars(nodes Expr) []string {
//	vars := make([]string, 0, 2)
//	WalkFunc(nodes, func(nodes Expr) {
//		switch typ := nodes.(type) {
//		case *CallExpr:
//			if typ.Fun.IsGeospatial() {
//				for _, arg := range typ.Args {
//					lit, ok := arg.(*StringLit)
//					if !ok {
//						continue
//					}
//					vars = append(vars, lit.Value[1:])
//				}
//			}
//		}
//	})
//	return vars
//}
