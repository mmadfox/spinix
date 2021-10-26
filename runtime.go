package spinix

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"time"

	"github.com/tidwall/geojson"

	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/geojson/geometry"
)

const (
	// Minimum distance in meters.
	// Used to round off the distance to avoid noise.
	minDistMeters = 50

	numBucket = 256
)

type evaluater interface {
	refIDs() map[string]Token
	evaluate(ctx context.Context, device *Device, state *State, ref reference) (Match, error)
}

type reference struct {
	rules   Rules
	objects Objects
	devices Devices
	states  States
}

type Match struct {
	Ok       bool  `json:"ok"`
	Left     Decl  `json:"left"`
	Right    Decl  `json:"right"`
	Operator Token `json:"operator"`
	Pos      Pos   `json:"pos"`
}

type Decl struct {
	Keyword Token
	Refs    []string
}

func defaultRefs() reference {
	return reference{
		devices: NewMemoryDevices(),
		objects: NewMemoryObjects(),
		rules:   NewMemoryRules(),
		states:  NewMemoryState(),
	}
}

type spec struct {
	nodes         []evaluater
	ops           []Token
	pos           Pos
	isStateful    bool
	resetInterval time.Duration
	times         int
	repeat        RepeatMode
	interval      time.Duration
	delay         time.Duration
	center        geometry.Point
	expire        time.Duration
	radius        float64
}

func specFromString(s string) (*spec, error) {
	expr, err := ParseSpec(s)
	if err != nil {
		return nil, err
	}
	return exprToSpec(expr)
}

func (s *spec) changeState(state *State) {
	state.UpdateLastSeenTime()
	switch s.repeat {
	case RepeatTimes, RepeatOnce:
		state.HitIncr()
	}
}

func (s *spec) checkTrigger(state *State, device *Device) bool {
	switch s.repeat {
	case RepeatEvery:
		if state.lastSeenTime == 0 {
			return true
		}
		currTime := mapper{device: device}.dateTime()
		dur := currTime.Unix() - state.LastResetTime()
		return dur > int64(s.delay.Seconds())
	case RepeatTimes:
		currTime := mapper{device: device}.dateTime()
		dur := currTime.Unix() - state.LastSeenTime()
		if dur < int64(s.interval.Seconds()) {
			return false
		}
		return state.Hits() < s.times
	case RepeatOnce:
		return state.hits == 0
	}
	return true
}

func (s *spec) evaluate(ctx context.Context, ruleID string, d *Device, r reference) (matches []Match, ok bool, err error) {
	if d == nil {
		return matches, false, nil
	}

	if len(s.nodes) == 0 {
		return
	}

	var currState *State
	if s.isStateful {
		sid := StateID{IMEI: d.IMEI, RuleID: ruleID}
		currState, err = r.states.Lookup(ctx, sid)
		if err != nil {
			if errors.Is(err, ErrStateNotFound) {
				if currState, err = r.states.Make(ctx, sid); err != nil {
					return
				}
				err = nil
			} else {
				return
			}
		}

		currState.SetTime(time.Now().Unix())

		if currState.NeedReset(s.resetInterval) {
			currState.Reset()
			currState.UpdateLastResetTime()
		}

		if ok := s.checkTrigger(currState, d); !ok {
			return nil, false, nil
		}
	}

	if len(s.nodes) == 1 {
		match, err := s.nodes[0].evaluate(ctx, d, currState, r)
		if err != nil {
			return nil, false, err
		}
		if s.isStateful && currState != nil {
			s.changeState(currState)
			if err = r.states.Update(ctx, currState); err != nil {
				return nil, false, err
			}
		}
		if match.Ok {
			return []Match{match}, true, nil
		}
	}

	var (
		index int
		op    Token
	)

	for index < len(s.nodes) {
		var right Match
		node := s.nodes[index]
		if node == nil {
			continue
		}

		if index > 0 {
			if !ok && op == AND {
				if index < len(s.ops) {
					op = s.ops[index]
				}
				index++
				continue
			}
		}

		right, err = s.nodes[index].evaluate(ctx, d, currState, r)
		if err != nil {
			return nil, false, err
		}
		if index < len(s.ops) {
			op = s.ops[index]
		}
		switch op {
		case AND:
			if index == 0 {
				ok = right.Ok
			} else {
				ok = ok && right.Ok
			}
		case OR:
			if index == 0 {
				ok = right.Ok
			} else {
				ok = ok || right.Ok
			}
		}
		if right.Ok {
			if matches == nil {
				matches = make([]Match, 0, len(s.nodes))
			}
			matches = append(matches, right)
		}
		index++
	}
	if s.isStateful && currState != nil {
		s.changeState(currState)
		err = r.states.Update(ctx, currState)
	}
	return
}

func walkExpr(
	expr Expr,
	exprFunc func(a, b Expr, op Token) error,
	opFunc func(tok Token),
) (Expr, error) {
	if expr == nil {
		return nil, fmt.Errorf("spinix/runtime: expression is nil")
	}
	switch n := expr.(type) {
	case *ParenExpr:
		return walkExpr(n.Expr, exprFunc, opFunc)
	case *BinaryExpr:
		lhs, err := walkExpr(n.LHS, exprFunc, opFunc)
		if err != nil {
			return nil, err
		}
		if n.Op == AND || n.Op == OR {
			opFunc(n.Op)
		}
		rhs, err := walkExpr(n.RHS, exprFunc, opFunc)
		if err != nil {
			return nil, err
		}
		_, lbe := lhs.(*BinaryExpr)
		_, rbe := rhs.(*BinaryExpr)
		if !lbe && !rbe {
			if err := exprFunc(lhs, rhs, n.Op); err != nil {
				return nil, err
			}
		}
	}
	return expr, nil
}

func isStateful(e Expr) bool {
	switch expr := e.(type) {
	case *ObjectLit:
		switch expr.DurTyp {
		case DURATION, AFTER:
			if expr.DurVal > 0 {
				return true
			}
		}
	}
	return false
}

func setupProps(s *spec, expr *PropExpr) {
	s.isStateful = true
	for i := 0; i < len(expr.List); i++ {
		switch prop := expr.List[i].(type) {
		case *PointLit:
			switch prop.Kind {
			case CENTER:
				s.center = geometry.Point{X: prop.Lat, Y: prop.Lon}
			}
		case *BaseLit:
			switch prop.Kind {
			case RADIUS:
				distLit, ok := prop.Expr.(*DistanceLit)
				if !ok {
					continue
				}
				if distLit.Unit == DistanceKilometers {
					distLit.Value *= 1000
				}
				s.radius = distLit.Value
			case EXPIRE:
				durLit, ok := prop.Expr.(*DurationLit)
				if !ok {
					continue
				}
				s.expire = durLit.Value
			}
		case *ResetLit:
			s.resetInterval = prop.After
		case *TriggerLit:
			s.repeat = prop.Repeat
			s.delay = prop.Value
			s.times = prop.Times
			s.interval = prop.Interval
		}
	}
	if s.resetInterval == 0 {
		s.resetInterval = 24 * time.Hour
	}
}

func exprToSpec(e Expr) (*spec, error) {
	s := &spec{ops: make([]Token, 0, 2), nodes: make([]evaluater, 0, 2)}
	if propExpr, ok := e.(*PropExpr); ok {
		setupProps(s, propExpr)
		e = propExpr.Expr
	}
	_, err := walkExpr(e,
		func(a, b Expr, op Token) error {
			if isStateful(a) {
				s.isStateful = true
			}
			if isStateful(b) {
				s.isStateful = true
			}
			node, err := makeOp(a, b, op)
			if err != nil {
				return err
			}
			s.nodes = append(s.nodes, node)
			return nil
		}, func(tok Token) {
			s.ops = append(s.ops, tok)
		})
	if err != nil {
		return nil, err
	}
	if len(s.nodes) == 0 {
		return nil, fmt.Errorf("spinix/runtime: specification not defined")
	}
	if len(s.nodes)-1 != len(s.ops) {
		return nil, fmt.Errorf("spinix/runtime: invalid specification %s", e)
	}
	return s, nil
}

func makeOp(left, right Expr, op Token) (evaluater, error) {
	switch op {
	case INTERSECTS:
		return e2intersects(left, right, false)
	case NINTERSECTS:
		return e2intersects(left, right, true)
	case NEAR:
		return e2near(left, right)
	case RANGE:
		return e2range(left, right, false)
	case NRANGE:
		return e2range(left, right, true)
	case IN:
		return e2in(left, right, false)
	case NIN:
		return e2in(left, right, true)
	case EQ:
		return e2equal(left, right, EQ)
	case LT:
		return e2equal(left, right, LT)
	case GT:
		return e2equal(left, right, GT)
	case NE:
		return e2equal(left, right, NE)
	case LTE:
		return e2equal(left, right, LTE)
	case GTE:
		return e2equal(left, right, GTE)
	}
	return nil, fmt.Errorf("spinix/runtime: illegal %v %v %v", left, op, right)
}

func e2intersects(left, right Expr, not bool) (evaluater, error) {
	op := INTERSECTS
	if not {
		op = NINTERSECTS
	}
	// device -> devices
	// device -> objects(polygon, circle, rect, ...)
	// object -> device
	// devices -> device
	switch lhs := left.(type) {
	case *DeviceLit:
		switch rhs := right.(type) {
		case *ObjectLit:
			if !isObjectToken(rhs.Kind) {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: rhs,
					Op:    op,
					Pos:   rhs.Pos,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						rhs.Kind, group2str(objectTokenGroup)),
				}
			}
			return intersectsObjectOp{
				left:  lhs,
				right: rhs,
				pos:   rhs.Pos,
				not:   not,
			}, nil
		case *DevicesLit:
			return intersectsDevicesOp{
				left:  lhs,
				right: rhs,
				pos:   rhs.Pos,
				not:   not,
			}, nil
		}
	case *ObjectLit:
		if !isObjectToken(lhs.Kind) {
			return nil, &InvalidExprError{
				Left:  lhs,
				Right: right,
				Op:    op,
				Pos:   lhs.Pos,
				Msg: fmt.Sprintf("got %s, expected [%s]",
					lhs.Kind, group2str(objectTokenGroup)),
			}
		}
		switch rhs := right.(type) {
		case *DeviceLit:
			return intersectsObjectOp{
				left:  rhs,
				right: lhs,
				pos:   rhs.Pos,
				not:   not,
			}, nil
		}
	case *DevicesLit:
		switch rhs := right.(type) {
		case *DeviceLit:
			return intersectsDevicesOp{
				left:  rhs,
				right: lhs,
				pos:   rhs.Pos,
				not:   not,
			}, nil
		}
	}
	return nil, &InvalidExprError{
		Left:  left,
		Right: right,
		Op:    op,
		Msg:   "illegal",
	}
}

func e2in(left, right Expr, not bool) (evaluater, error) {
	op := IN
	if not {
		op = NIN
	}
	switch lhs := left.(type) {
	// device -> objects(polygon, circle, rect, ...)
	case *DeviceLit:
		switch lhs.Unit {
		case DistanceMeters, DistanceKilometers:
			if lhs.Value <= 0 {
				return nil, &InvalidExprError{
					Left:  left,
					Right: right,
					Op:    op,
					Pos:   lhs.Pos,
					Msg:   fmt.Sprintf("invalid distance value %s", lhs.Kind),
				}
			}
		}

		switch rhs := right.(type) {
		case *ObjectLit:
			if !isObjectToken(rhs.Kind) {
				return nil, &InvalidExprError{
					Left:  left,
					Right: right,
					Op:    op,
					Pos:   rhs.Pos,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						rhs.Kind, group2str(objectTokenGroup)),
				}
			}
			return inObjectOp{
				device: lhs,
				object: rhs,
				pos:    lhs.Pos,
				not:    not,
			}, nil
		}

	// ident -> int, float, string
	case *IdentLit:
		rhs, ok := right.(*ListLit)
		if !ok || rhs.Kind != ILLEGAL {
			return nil, &InvalidExprError{
				Left:  left,
				Right: right,
				Op:    op,
				Msg:   "expected [left .. right]",
			}
		}

		switch rhs.Typ {
		case INT:
			if !isNumberToken(lhs.Kind) {
				return nil, &InvalidExprError{
					Left:  left,
					Right: right,
					Op:    op,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						lhs.Kind, group2str(numberTokenGroup)),
				}
			}
			op := inIntOp{
				keyword: lhs.Kind,
				pos:     rhs.Pos,
				values:  make(map[int]struct{}),
				not:     not,
			}
			for i := 0; i < len(rhs.Items); i++ {
				n := rhs.Items[i].(*IntLit)
				op.values[n.Value] = struct{}{}
			}
			return op, nil
		case FLOAT:
			if !isNumberToken(lhs.Kind) {
				return nil, &InvalidExprError{
					Left:  left,
					Right: right,
					Op:    op,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						lhs.Kind, group2str(numberTokenGroup)),
				}
			}
			op := inFloatOp{
				keyword: lhs.Kind,
				pos:     rhs.Pos,
				values:  make(map[float64]struct{}),
				not:     not,
			}
			for i := 0; i < len(rhs.Items); i++ {
				n := rhs.Items[i].(*FloatLit)
				op.values[n.Value] = struct{}{}
			}
			return op, nil
		case STRING, IDENT:
			if !isStringToken(lhs.Kind) {
				return nil, &InvalidExprError{
					Left:  left,
					Right: right,
					Op:    op,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						lhs.Kind, group2str(stringTokenGroup)),
				}
			}
			op := inStringOp{
				keyword: lhs.Kind,
				pos:     rhs.Pos,
				values:  make(map[string]struct{}),
				not:     not,
			}
			for i := 0; i < len(rhs.Items); i++ {
				n := rhs.Items[i].(*StringLit)
				op.values[n.Value] = struct{}{}
			}
			return op, nil
		}
	}

	return nil, &InvalidExprError{
		Left:  left,
		Right: right,
		Op:    op,
		Msg:   "illegal",
	}
}

func e2range(left, right Expr, not bool) (evaluater, error) {
	// int -> int
	// float -> float
	// time -> time
	// dateTime -> dateTime
	op := RANGE
	if not {
		op = NRANGE
	}
	switch lhs := left.(type) {
	case *IdentLit:
		switch rhs := right.(type) {
		case *ListLit:
			if rhs.Kind != RANGE {
				return nil, &InvalidExprError{
					Left:  left,
					Right: right,
					Pos:   rhs.Pos,
					Op:    op,
				}
			}
			switch rhs.Typ {
			case INT:
				if !isNumberToken(lhs.Kind) {
					return nil, &InvalidExprError{
						Left:  left,
						Right: right,
						Op:    op,
						Pos:   rhs.Pos,
						Msg: fmt.Sprintf("got %s, expected [%s]",
							lhs.Kind, group2str(numberTokenGroup)),
					}
				}
				begin := rhs.Items[0].(*IntLit)
				end := rhs.Items[1].(*IntLit)
				if begin.Value > end.Value {
					return nil, &InvalidExprError{
						Left:  left,
						Right: right,
						Op:    op,
						Pos:   rhs.Pos,
						Msg:   "left operand is greater than right",
					}
				}
				if begin.Value == end.Value {
					return nil, &InvalidExprError{
						Left:  left,
						Right: right,
						Op:    op,
						Pos:   rhs.Pos,
						Msg:   "left and right operands are equal",
					}
				}
				return rangeIntOp{
					begin:   begin.Value,
					end:     end.Value,
					pos:     rhs.Pos,
					keyword: lhs.Kind,
					not:     not,
				}, nil
			case FLOAT:
				if !isNumberToken(lhs.Kind) {
					return nil, &InvalidExprError{
						Left:  left,
						Right: right,
						Op:    op,
						Pos:   rhs.Pos,
						Msg: fmt.Sprintf("got %s, expected [%s]",
							lhs.Kind, group2str(numberTokenGroup)),
					}
				}
				begin := rhs.Items[0].(*FloatLit)
				end := rhs.Items[1].(*FloatLit)
				if begin.Value > end.Value {
					return nil, &InvalidExprError{
						Left:  left,
						Right: right,
						Op:    op,
						Pos:   rhs.Pos,
						Msg:   "left operand is greater than right",
					}
				}
				if begin.Value == end.Value {
					return nil, &InvalidExprError{
						Left:  left,
						Right: right,
						Op:    op,
						Pos:   rhs.Pos,
						Msg:   "left and right operands are equal",
					}
				}
				return rangeFloatOp{
					begin:   begin.Value,
					end:     end.Value,
					pos:     rhs.Pos,
					keyword: lhs.Kind,
					not:     not,
				}, nil
			case TIME:
				if !isTimeToken(lhs.Kind) {
					return nil, &InvalidExprError{
						Left:  left,
						Right: right,
						Op:    op,
						Pos:   rhs.Pos,
						Msg:   fmt.Sprintf("got %s, expected %s", lhs.Kind, TIME),
					}
				}
				begin := rhs.Items[0].(*TimeLit)
				end := rhs.Items[1].(*TimeLit)
				if begin.Hour < 0 || begin.Hour > 23 {
					return nil, fmt.Errorf("spinix/runtime: invalid expr: got %d, expected hour >= 0 and hour < 24, pos=%d",
						begin.Hour, rhs.Pos)
				}
				if begin.Minute < 0 || begin.Minute > 59 {
					return nil, fmt.Errorf("spinix/runtime: invalid expr: got %d, expected minutes >= 0 and minutes < 59, pos=%d",
						begin.Minute, rhs.Pos)
				}
				if end.Hour < 0 || end.Hour > 23 {
					return nil, fmt.Errorf("spinix/runtime: invalid expr: got %d, expected hour >= 0 and hour < 24, pos=%d",
						end.Hour, rhs.Pos)
				}
				if end.Minute < 0 || end.Minute > 59 {
					return nil, fmt.Errorf("spinix/runtime: invalid expr: got %d, expected minutes >= 0 and minutes < 59, pos=%d",
						end.Minute, rhs.Pos)
				}
				return rangeTimeOp{
					begin:   timeVal{h: begin.Hour, m: begin.Minute},
					end:     timeVal{h: end.Hour, m: end.Minute},
					pos:     rhs.Pos,
					keyword: lhs.Kind,
					not:     not,
				}, nil
			case STRING:
				if lhs.Kind != DATE && lhs.Kind != DATETIME {
					break
				}
				begin, ok := rhs.Items[0].(*StringLit)
				if !ok {
					break
				}
				end, ok := rhs.Items[1].(*StringLit)
				if !ok {
					break
				}
				var pattern string
				switch lhs.Kind {
				case DATE:
					pattern = "2006-01-02"
				default:
					pattern = time.RFC3339
				}

				beginValue := strings.ReplaceAll(begin.Value, `"`, "")
				left, err := time.Parse(pattern, beginValue)
				if err != nil {
					break
				}
				endValue := strings.ReplaceAll(end.Value, `"`, "")
				right, err := time.Parse(pattern, endValue)
				if err != nil {
					break
				}

				// left == right
				if lhs.Kind == DATETIME && left.Unix() == right.Unix() {
					return nil, fmt.Errorf("spinix/runtime: invalid expr: left %s and right %s operands are equal, pos=%d",
						left, right, rhs.Pos)
				}
				// left > right
				if lhs.Kind == DATETIME && left.Unix() > right.Unix() {
					return nil, fmt.Errorf("spinix/runtime: invalid expr: left %s operand is greater than right %s, pos=%d",
						left, right, rhs.Pos)
				}
				return rangeDateTimeOp{
					keyword: lhs.Kind,
					begin:   left,
					end:     right,
					pos:     rhs.Pos,
					not:     not,
				}, nil
			}
		}
	}
	return nil, &InvalidExprError{
		Left:  left,
		Right: right,
		Op:    op,
	}
}

func e2equal(left, right Expr, op Token) (evaluater, error) {
	// ident -> int
	// ident -> float
	// ident -> string
	// ident -> time
	// int -> ident
	// float -> ident
	// string -> ident
	// time -> ident
	// device -> objects(polygon, rect, circle, ...)
	// device -> devices
	// object -> device
	// devices -> device

	// left
	switch lhs := left.(type) {
	// device -> objects, devices
	case *DeviceLit:
		switch rhs := right.(type) {
		case *ObjectLit:
			if !isObjectToken(rhs.Kind) {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: rhs,
					Pos:   rhs.Pos,
					Op:    op,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						rhs.Kind, group2str(objectTokenGroup)),
				}
			}
			return equalObjectOp{
				left:  lhs,
				right: rhs,
				op:    op,
				pos:   rhs.Pos,
			}, nil
		case *DevicesLit:
			return equalDevicesOp{
				left:  lhs,
				right: rhs,
				pos:   rhs.Pos,
				op:    op,
			}, nil
		}
	// devices -> device
	case *DevicesLit:
		switch rhs := right.(type) {
		case *DeviceLit:
			return equalDevicesOp{
				left:  rhs,
				right: lhs,
				pos:   rhs.Pos,
				op:    op,
			}, nil
		}
	// object -> device
	case *ObjectLit:
		switch rhs := right.(type) {
		case *DeviceLit:
			return equalObjectOp{
				left:  rhs,
				right: lhs,
				pos:   rhs.Pos,
				op:    op,
			}, nil
		}
	// time -> ident
	case *TimeLit:
		switch rhs := right.(type) {
		case *IdentLit:
			if !isTimeToken(rhs.Kind) {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: rhs,
					Op:    op,
					Pos:   rhs.Pos,
					Msg:   fmt.Sprintf("got %s, expected %s", rhs.Kind, TIME),
				}
			}
			return equalTimeOp{
				keyword: rhs.Kind,
				op:      op,
				value: timeVal{
					h: lhs.Hour,
					m: lhs.Minute,
				}, pos: rhs.Pos,
			}, nil
		}
	// string -> ident
	case *StringLit:
		switch rhs := right.(type) {
		case *IdentLit:
			if !isStringToken(rhs.Kind) {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: rhs,
					Op:    op,
					Pos:   rhs.Pos,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						rhs.Kind, group2str(stringTokenGroup)),
				}
			}
			return equalStrOp{keyword: rhs.Kind, value: lhs.Value, pos: rhs.Pos, op: op}, nil
		}
	// float -> ident
	case *FloatLit:
		switch rhs := right.(type) {
		case *IdentLit:
			if !isNumberToken(rhs.Kind) {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: rhs,
					Op:    op,
					Pos:   rhs.Pos,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						rhs.Kind, group2str(numberTokenGroup)),
				}
			}
			return equalFloatOp{keyword: rhs.Kind, value: lhs.Value, pos: rhs.Pos, op: op}, nil
		}
	// int -> ident
	case *IntLit:
		switch rhs := right.(type) {
		case *IdentLit:
			if !isNumberToken(rhs.Kind) {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: rhs,
					Op:    op,
					Pos:   rhs.Pos,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						rhs.Kind, group2str(numberTokenGroup)),
				}
			}
			return equalIntOp{keyword: rhs.Kind, value: lhs.Value, pos: rhs.Pos, op: op}, nil
		}
	case *IdentLit:
		switch rhs := right.(type) {
		// ident -> int
		case *IntLit:
			if !isNumberToken(lhs.Kind) {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: rhs,
					Op:    op,
					Pos:   rhs.Pos,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						lhs.Kind, group2str(numberTokenGroup)),
				}
			}
			return equalIntOp{keyword: lhs.Kind, value: rhs.Value, pos: rhs.Pos, op: op}, nil
		case *FloatLit:
			if !isNumberToken(lhs.Kind) {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: rhs,
					Op:    op,
					Pos:   rhs.Pos,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						lhs.Kind, group2str(numberTokenGroup)),
				}
			}
			return equalFloatOp{keyword: lhs.Kind, value: rhs.Value, pos: rhs.Pos, op: op}, nil
		case *StringLit:
			if !isStringToken(lhs.Kind) {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: rhs,
					Op:    op,
					Pos:   rhs.Pos,
					Msg: fmt.Sprintf("got %s, expected [%s]",
						lhs.Kind, group2str(stringTokenGroup)),
				}
			}
			return equalStrOp{keyword: lhs.Kind, value: rhs.Value, pos: rhs.Pos, op: op}, nil
		case *TimeLit:
			if !isTimeToken(lhs.Kind) {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: rhs,
					Op:    op,
					Pos:   rhs.Pos,
					Msg: fmt.Sprintf("got %s, expected %s",
						lhs.Kind, TIME),
				}
			}
			return equalTimeOp{
				keyword: lhs.Kind,
				value:   timeVal{h: rhs.Hour, m: rhs.Minute},
				pos:     rhs.Pos,
				op:      op,
			}, nil
		}
	}
	return nil, &InvalidExprError{
		Left:  left,
		Right: right,
		Op:    op,
		Msg:   "illegal",
	}
}

func e2near(left, right Expr) (evaluater, error) {
	var node nearOp
	// device -> object
	// device -> devices
	// object -> device
	// devices -> device
	switch lhs := left.(type) {
	case *DeviceLit:
		node.device = lhs
		switch rhs := right.(type) {
		case *ObjectLit:
			node.object = rhs
			node.pos = rhs.Pos
		case *DevicesLit:
			node.devices = rhs
			node.pos = rhs.Pos
		case *DeviceLit:
			node.other = rhs
			node.pos = rhs.Pos
			switch node.device.Unit {
			case DistanceMeters, DistanceKilometers:
			default:
				node.device.Unit = DistanceMeters
				node.device.Value = 1000
			}
		default:
			return node, fmt.Errorf("spinix/runtime: invalid specStr => %s NEAR %s, pos=%v",
				lhs, rhs, lhs.Pos)
		}
	case *ObjectLit:
		node.object = lhs
		switch rhs := right.(type) {
		case *DeviceLit:
			node.device = rhs
			node.pos = rhs.Pos
		default:
			return node, fmt.Errorf("spinix/runtime: invalid specStr => %s NEAR %s, pos=%v",
				lhs, rhs, lhs.Pos)
		}
	case *DevicesLit:
		node.devices = lhs
		switch rhs := right.(type) {
		case *DeviceLit:
			node.device = rhs
			node.pos = rhs.Pos
		default:
			return node, fmt.Errorf("spinix/runtime: invalid specStr => %s NEAR %s, pos=%v",
				lhs, rhs, lhs.Pos)
		}
	default:
		return node, fmt.Errorf("spinix/runtime: invalid specStr => %s NEAR %s",
			left, right)
	}

	switch node.device.Unit {
	case DistanceMeters, DistanceKilometers:
		if node.device.Value <= 0 {
			return node, fmt.Errorf("spinix/runtime: invalid distance value in specStr => %s, operator=%v, pos=%v",
				node.device, NEAR, node.device.Pos)
		}
	}
	if node.devices != nil && node.object != nil && node.other != nil {
		return node, fmt.Errorf("spinix/runtime: invalid specStr => %s NEAR %s",
			left, right)
	}
	return node, nil
}

type nearOp struct {
	// left
	device *DeviceLit

	// right
	object  *ObjectLit
	devices *DevicesLit
	other   *DeviceLit

	pos Pos
}

func (n nearOp) refIDs() (refs map[string]Token) {
	if n.object != nil && len(n.object.Ref) > 0 {
		refs = make(map[string]Token)
		for i := 0; i < len(n.object.Ref); i++ {
			refs[n.object.Ref[i]] = n.object.Kind
		}
	}
	if n.devices != nil && len(n.devices.Ref) > 0 {
		refs = make(map[string]Token)
		for i := 0; i < len(n.devices.Ref); i++ {
			refs[n.devices.Ref[i]] = n.devices.Kind
		}
	}
	return
}

func (n nearOp) evaluate(ctx context.Context, d *Device, _ *State, ref reference) (match Match, err error) {
	// device
	var (
		meters       float64
		deviceRadius *geometry.Poly
		devicePoint  geometry.Point
	)

	switch n.device.Unit {
	case DistanceKilometers:
		if n.device.Value > 0 {
			meters = n.device.Value * 1000
		}
	case DistanceMeters:
		meters = n.device.Value
	}

	switch n.device.Kind {
	case RADIUS, BBOX:
		// circle or rect
		ring := makeRadiusRing(d.Latitude, d.Longitude, meters, 16)
		deviceRadius = &geometry.Poly{Exterior: ring}
	default:
		// point
		devicePoint = geometry.Point{X: d.Latitude, Y: d.Longitude}
	}

	// device -> objects(polygon, circle, rect, ...)
	if n.object != nil {
		for i := 0; i < len(n.object.Ref); i++ {
			objectID := n.object.Ref[i]
			obj, err := ref.objects.Lookup(ctx, objectID)
			if err != nil {
				if errors.Is(err, ErrObjectNotFound) {
					continue
				}
				return match, err
			}
			if obj == nil {
				return match, nil
			}
			switch n.device.Kind {
			case RADIUS:
				if deviceRadius != nil && obj.Spatial().IntersectsPoly(deviceRadius) {
					match.Ok = true
					if match.Right.Refs == nil {
						match.Right.Refs = make([]string, 0, len(n.object.Ref))
					}
					match.Right.Refs = append(match.Right.Refs, objectID)
				}
			case BBOX:
				if deviceRadius != nil && obj.Spatial().IntersectsRect(deviceRadius.Rect()) {
					match.Ok = true
					if match.Right.Refs == nil {
						match.Right.Refs = make([]string, 0, len(n.object.Ref))
					}
					match.Right.Refs = append(match.Right.Refs, objectID)
				}
			default:
				if obj.Spatial().IntersectsPoint(devicePoint) {
					match.Ok = true
					if match.Right.Refs == nil {
						match.Right.Refs = make([]string, 0, len(n.object.Ref))
					}
					match.Right.Refs = append(match.Right.Refs, objectID)
				}
			}
		}
		if match.Ok {
			match.Left.Keyword = DEVICE
			match.Left.Refs = []string{d.IMEI}
			match.Operator = NEAR
			match.Pos = n.pos
			match.Right.Keyword = OBJECTS
		}
		return match, nil
	}

	// device -> devices
	if n.devices != nil {
		var otherDeviceMeters float64
		switch n.devices.Unit {
		case DistanceKilometers:
			if n.devices.Value > 0 {
				otherDeviceMeters = n.devices.Value * 1000
			}
		case DistanceMeters:
			otherDeviceMeters = n.devices.Value
		}
		var otherDeviceRadius *geometry.Poly
		var otherDevicePoint geometry.Point
		for _, otherDeviceID := range n.devices.Ref {
			otherDevice, err := ref.devices.Lookup(ctx, otherDeviceID)
			if err != nil {
				if errors.Is(err, ErrDeviceNotFound) {
					continue
				}
				return match, err
			}
			switch n.devices.Kind {
			case RADIUS, BBOX:
				// circle
				ring := makeRadiusRing(
					otherDevice.Latitude,
					otherDevice.Longitude,
					otherDeviceMeters, 16)
				otherDeviceRadius = &geometry.Poly{Exterior: ring}
				switch n.devices.Kind {
				case RADIUS:
					if deviceRadius != nil && otherDeviceRadius.IntersectsPoly(deviceRadius) {
						match.Ok = true
						if match.Right.Refs == nil {
							match.Right.Refs = make([]string, 0, len(n.devices.Ref))
						}
						match.Right.Refs = append(match.Right.Refs, otherDeviceID)
					}
				case BBOX:
					if deviceRadius != nil && otherDeviceRadius.IntersectsRect(deviceRadius.Rect()) {
						match.Ok = true
						if match.Right.Refs == nil {
							match.Right.Refs = make([]string, 0, len(n.devices.Ref))
						}
						match.Right.Refs = append(match.Right.Refs, otherDeviceID)
					}
				default:
					if otherDeviceRadius.IntersectsPoint(devicePoint) {
						match.Ok = true
						if match.Right.Refs == nil {
							match.Right.Refs = make([]string, 0, len(n.devices.Ref))
						}
						match.Right.Refs = append(match.Right.Refs, otherDeviceID)
					}
				}
			default:
				// point
				otherDevicePoint = geometry.Point{
					X: otherDevice.Latitude,
					Y: otherDevice.Longitude,
				}
				if otherDevicePoint.IntersectsPoint(devicePoint) {
					match.Ok = true
					if match.Right.Refs == nil {
						match.Right.Refs = make([]string, 0, len(n.devices.Ref))
					}
					match.Right.Refs = append(match.Right.Refs, otherDeviceID)
				}
			}
		}
		if match.Ok {
			match.Left.Keyword = DEVICE
			match.Left.Refs = []string{d.IMEI}
			match.Operator = NEAR
			match.Pos = n.pos
			match.Right.Keyword = DEVICES
		}
		return match, nil
	}

	// device -> device
	if n.other != nil {
		err := ref.devices.Nearby(ctx, d.Latitude, d.Longitude, meters,
			func(ctx context.Context, other *Device) error {
				if d.IMEI == other.IMEI {
					return nil
				}
				match.Ok = true
				if match.Right.Refs == nil {
					match.Right.Refs = make([]string, 0, 8)
				}
				match.Right.Refs = append(match.Right.Refs, other.IMEI)
				return nil
			})
		if err != nil {
			return match, err
		}
		if match.Ok {
			match.Left.Keyword = DEVICE
			match.Left.Refs = []string{d.IMEI}
			match.Operator = NEAR
			match.Pos = n.pos
			match.Right.Keyword = DEVICE
		}
		return match, nil
	}

	return
}

type rangeDateTimeOp struct {
	keyword Token
	begin   time.Time
	end     time.Time
	pos     Pos
	not     bool
}

func (n rangeDateTimeOp) refIDs() (refs map[string]Token) { return }

func (n rangeDateTimeOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	values := mapper{device: d}
	ts := values.dateTime()
	if n.not {
		match.Ok = ts.Unix() <= n.begin.Unix() || ts.Unix() >= n.end.Unix()
		match.Operator = NRANGE
	} else {
		match.Ok = ts.Unix() >= n.begin.Unix() && ts.Unix() <= n.end.Unix()
		match.Operator = RANGE
	}
	match.Left.Keyword = n.keyword
	match.Right.Keyword = DATETIME
	match.Pos = n.pos
	return
}

type timeVal struct {
	h, m int
}

type rangeTimeOp struct {
	keyword Token
	begin   timeVal
	end     timeVal
	pos     Pos
	not     bool
}

func (n rangeTimeOp) refIDs() (refs map[string]Token) { return }

func (n rangeTimeOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	values := mapper{device: d}
	ts := values.dateTime()
	d1 := time.Date(ts.Year(), ts.Month(), ts.Day(), n.begin.h, n.begin.m, 0, 0, ts.Location())
	d2 := time.Date(ts.Year(), ts.Month(), ts.Day(), n.end.h, n.end.m, 0, 0, ts.Location())
	if n.not {
		match.Ok = ts.Unix() <= d1.Unix() || ts.Unix() >= d2.Unix()
		match.Operator = NRANGE
	} else {
		match.Ok = ts.Unix() >= d1.Unix() && ts.Unix() <= d2.Unix()
		match.Operator = RANGE
	}
	match.Left.Keyword = n.keyword
	match.Right.Keyword = TIME
	match.Pos = n.pos
	return
}

type rangeIntOp struct {
	keyword Token
	begin   int
	end     int
	pos     Pos
	not     bool
}

func (n rangeIntOp) refIDs() (refs map[string]Token) { return }

func (n rangeIntOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	values := mapper{device: d}
	v := values.intVal(n.keyword)
	if n.not {
		match.Ok = v <= n.begin || v >= n.end
		match.Operator = NRANGE
	} else {
		match.Ok = v >= n.begin && v <= n.end
		match.Operator = RANGE
	}
	match.Left.Keyword = n.keyword
	match.Right.Keyword = INT
	match.Pos = n.pos

	return
}

type rangeFloatOp struct {
	keyword Token
	begin   float64
	end     float64
	pos     Pos
	not     bool
}

func (n rangeFloatOp) refIDs() (refs map[string]Token) { return }

func (n rangeFloatOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	values := mapper{device: d}
	v := values.floatVal(n.keyword)
	if n.not {
		match.Ok = v <= n.begin || v >= n.end
		match.Operator = NRANGE
	} else {
		match.Ok = v >= n.begin && v <= n.end
		match.Operator = RANGE
	}
	match.Left.Keyword = n.keyword
	match.Right.Keyword = FLOAT
	match.Pos = n.pos
	return
}

type inFloatOp struct {
	keyword Token
	pos     Pos
	values  map[float64]struct{}
	not     bool
}

func (n inFloatOp) refIDs() (refs map[string]Token) { return }

func (n inFloatOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	value := mapper{device: d}.floatVal(n.keyword)
	_, found := n.values[value]
	match.Left.Keyword = n.keyword
	match.Right.Keyword = FLOAT
	match.Pos = n.pos
	if n.not {
		match.Ok = !found
		match.Operator = NIN
	} else {
		match.Ok = found
		match.Operator = IN
	}
	return
}

type intersectsObjectOp struct {
	left  *DeviceLit
	right *ObjectLit
	pos   Pos
	not   bool
}

func (n intersectsObjectOp) refIDs() (refs map[string]Token) {
	if n.right != nil && len(n.right.Ref) > 0 {
		refs = make(map[string]Token)
		for i := 0; i < len(n.right.Ref); i++ {
			refs[n.right.Ref[i]] = n.right.Kind
		}
	}
	return
}

func (n intersectsObjectOp) evaluate(ctx context.Context, d *Device, _ *State, ref reference) (match Match, err error) {
	op := INTERSECTS
	if n.not {
		op = NINTERSECTS
	}
	var deviceRadius *geometry.Poly
	var devicePoint geometry.Point
	switch n.left.Kind {
	case RADIUS, BBOX:
		// circle or rect
		ring := makeRadiusRing(d.Latitude, d.Longitude, n.left.meters(), n.left.steps())
		deviceRadius = &geometry.Poly{Exterior: ring}
	default:
		// point
		devicePoint = geometry.Point{X: d.Latitude, Y: d.Longitude}
	}
	for i := 0; i < len(n.right.Ref); i++ {
		objectID := n.right.Ref[i]
		obj, err := ref.objects.Lookup(ctx, objectID)
		if err != nil {
			if errors.Is(err, ErrObjectNotFound) {
				continue
			}
			return match, err
		}
		if obj == nil {
			return match, nil
		}
		switch n.left.Kind {
		case RADIUS:
			if deviceRadius != nil && obj.Spatial().IntersectsPoly(deviceRadius) {
				match.Ok = true
				if match.Right.Refs == nil {
					match.Right.Refs = make([]string, 0, len(n.right.Ref))
				}
				match.Right.Refs = append(match.Right.Refs, objectID)
			}
		case BBOX:
			if deviceRadius != nil && obj.Spatial().IntersectsRect(deviceRadius.Rect()) {
				match.Ok = true
				if match.Right.Refs == nil {
					match.Right.Refs = make([]string, 0, len(n.right.Ref))
				}
				match.Right.Refs = append(match.Right.Refs, objectID)
			}
		default:
			if obj.Spatial().IntersectsPoint(devicePoint) {
				match.Ok = true
				if match.Right.Refs == nil {
					match.Right.Refs = make([]string, 0, len(n.right.Ref))
				}
				match.Right.Refs = append(match.Right.Refs, objectID)
			}
		}
	}
	if n.not {
		match.Ok = !match.Ok
	}
	if match.Ok {
		match.Left.Keyword = DEVICE
		match.Left.Refs = []string{d.IMEI}
		match.Operator = op
		match.Pos = n.pos
		match.Right.Keyword = n.right.Kind
	}
	return
}

type intersectsDevicesOp struct {
	left  *DeviceLit
	right *DevicesLit
	pos   Pos
	not   bool
}

func (n intersectsDevicesOp) refIDs() (refs map[string]Token) {
	if n.right != nil && len(n.right.Ref) > 0 {
		refs = make(map[string]Token)
		for i := 0; i < len(n.right.Ref); i++ {
			refs[n.right.Ref[i]] = n.right.Kind
		}
	}
	return
}

func (n intersectsDevicesOp) evaluate(ctx context.Context, d *Device, _ *State, ref reference) (match Match, err error) {
	op := INTERSECTS
	if n.not {
		op = NINTERSECTS
	}

	// left device
	var (
		meters       float64
		deviceRadius *geometry.Poly
		devicePoint  geometry.Point
	)

	switch n.left.Unit {
	case DistanceKilometers:
		if n.left.Value > 0 {
			meters = n.left.Value * 1000
		}
	case DistanceMeters:
		meters = n.left.Value
	}

	switch n.left.Kind {
	case RADIUS, BBOX:
		// circle or rect
		ring := makeRadiusRing(d.Latitude, d.Longitude, meters, 12)
		deviceRadius = &geometry.Poly{Exterior: ring}
	default:
		// point
		devicePoint = geometry.Point{X: d.Latitude, Y: d.Longitude}
	}

	// right devices
	var (
		otherDeviceMeters float64
		otherDeviceRadius *geometry.Poly
		otherDevicePoint  geometry.Point
	)

	switch n.right.Unit {
	case DistanceKilometers:
		if n.right.Value > 0 {
			otherDeviceMeters = n.right.Value * 1000
		}
	case DistanceMeters:
		otherDeviceMeters = n.right.Value
	}

	for _, otherDeviceID := range n.right.Ref {
		otherDevice, err := ref.devices.Lookup(ctx, otherDeviceID)
		if err != nil {
			if errors.Is(err, ErrDeviceNotFound) {
				continue
			}
			return match, err
		}

		switch n.right.Kind {
		case RADIUS, BBOX:
			// circle
			ring := makeRadiusRing(
				otherDevice.Latitude,
				otherDevice.Longitude,
				otherDeviceMeters, 12)
			otherDeviceRadius = &geometry.Poly{Exterior: ring}
			switch n.right.Kind {
			case RADIUS:
				if deviceRadius != nil && otherDeviceRadius.IntersectsPoly(deviceRadius) {
					match.Ok = true
					if match.Right.Refs == nil {
						match.Right.Refs = make([]string, 0, len(n.right.Ref))
					}
					match.Right.Refs = append(match.Right.Refs, otherDeviceID)
				}
			case BBOX:
				if deviceRadius != nil && otherDeviceRadius.IntersectsRect(deviceRadius.Rect()) {
					match.Ok = true
					if match.Right.Refs == nil {
						match.Right.Refs = make([]string, 0, len(n.right.Ref))
					}
					match.Right.Refs = append(match.Right.Refs, otherDeviceID)
				}
			default:
				if otherDeviceRadius.IntersectsPoint(devicePoint) {
					match.Ok = true
					if match.Right.Refs == nil {
						match.Right.Refs = make([]string, 0, len(n.right.Ref))
					}
					match.Right.Refs = append(match.Right.Refs, otherDeviceID)
				}
			}
		default:
			// point
			otherDevicePoint = geometry.Point{
				X: otherDevice.Latitude,
				Y: otherDevice.Longitude,
			}
			if otherDevicePoint.IntersectsPoint(devicePoint) {
				match.Ok = true
				if match.Right.Refs == nil {
					match.Right.Refs = make([]string, 0, len(n.right.Ref))
				}
				match.Right.Refs = append(match.Right.Refs, otherDeviceID)
			}
		}
	}

	if n.not {
		match.Ok = !match.Ok
	}

	if match.Ok {
		match.Left.Keyword = DEVICE
		match.Left.Refs = []string{d.IMEI}
		match.Operator = op
		match.Pos = n.pos
		match.Right.Keyword = DEVICES
	}
	return
}

type inObjectOp struct {
	device *DeviceLit
	object *ObjectLit
	pos    Pos
	not    bool
}

func (n inObjectOp) refIDs() (refs map[string]Token) {
	if n.object != nil && len(n.object.Ref) > 0 {
		refs = make(map[string]Token)
		for i := 0; i < len(n.object.Ref); i++ {
			refs[n.object.Ref[i]] = n.object.Kind
		}
	}
	return
}

func (n inObjectOp) evaluate(ctx context.Context, d *Device, _ *State, ref reference) (match Match, err error) {
	match.Left.Keyword = DEVICE
	match.Right.Keyword = n.object.Kind
	match.Pos = n.pos

	if n.not {
		match.Operator = NIN
	} else {
		match.Operator = IN
	}

	var deviceRadius *geojson.Polygon
	var devicePoint *geojson.Point

	if n.device.hasRadius() {
		ring := makeRadiusRing(d.Latitude, d.Longitude, n.device.meters(), n.device.steps())
		deviceRadius = geojson.NewPolygon(&geometry.Poly{Exterior: ring})
	} else {
		devicePoint = geojson.NewPoint(geometry.Point{X: d.Latitude, Y: d.Longitude})
	}

	for _, objectID := range n.object.Ref {
		object, err := ref.objects.Lookup(ctx, objectID)
		if err != nil {
			if errors.Is(err, ErrObjectNotFound) {
				continue
			}
			return match, err
		}

		if deviceRadius != nil {
			if ok := object.Contains(deviceRadius); ok {
				match.Ok = true
			}
		} else {
			if ok := object.Contains(devicePoint); ok {
				match.Ok = true
			}
		}
		if n.not {
			match.Ok = !match.Ok
		}
		if match.Ok {
			if match.Right.Refs == nil {
				match.Right.Refs = make([]string, 0, len(n.object.Ref))
			}
			match.Right.Refs = append(match.Right.Refs, objectID)
		}
	}
	return
}

type inIntOp struct {
	keyword Token
	pos     Pos
	values  map[int]struct{}
	not     bool
}

func (n inIntOp) refIDs() (refs map[string]Token) { return }

func (n inIntOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	value := mapper{device: d}.intVal(n.keyword)
	_, found := n.values[value]
	match.Left.Keyword = n.keyword
	match.Right.Keyword = INT
	match.Pos = n.pos
	if n.not {
		match.Ok = !found
		match.Operator = NIN
	} else {
		match.Ok = found
		match.Operator = IN
	}
	return
}

type inStringOp struct {
	keyword Token
	pos     Pos
	values  map[string]struct{}
	not     bool
}

func (n inStringOp) refIDs() (refs map[string]Token) { return }

func (n inStringOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	value := mapper{device: d}.stringVal(n.keyword)
	_, found := n.values[value]
	match.Left.Keyword = n.keyword
	match.Right.Keyword = STRING
	match.Pos = n.pos
	if n.not {
		match.Ok = !found
		match.Operator = NIN
	} else {
		match.Ok = found
		match.Operator = IN
	}
	return
}

type equalObjectOp struct {
	op    Token
	left  *DeviceLit
	right *ObjectLit
	pos   Pos
}

func (n equalObjectOp) refIDs() (refs map[string]Token) {
	if n.right != nil && len(n.right.Ref) > 0 {
		refs = make(map[string]Token)
		for i := 0; i < len(n.right.Ref); i++ {
			refs[n.right.Ref[i]] = n.right.Kind
		}
	}
	return
}

func (n equalObjectOp) evaluate(ctx context.Context, d *Device, _ *State, ref reference) (match Match, err error) {
	match.Left.Keyword = DEVICE
	match.Right.Keyword = n.right.Kind
	match.Pos = n.pos
	match.Operator = n.op
	for i := 0; i < len(n.right.Ref); i++ {
		objectID := n.right.Ref[i]
		object, err := ref.objects.Lookup(ctx, objectID)
		if err != nil {
			if errors.Is(err, ErrObjectNotFound) {
				continue
			}
			return match, err
		}
		center := object.Center()
		distance := round(geo.DistanceTo(
			d.Latitude,
			d.Longitude,
			center.X,
			center.Y), minDistMeters)
		switch n.op {
		case EQ:
			match.Ok = distance == n.left.meters()
		case LT:
			match.Ok = distance < n.left.meters()
		case GT:
			match.Ok = distance > n.left.meters()
		case NE:
			match.Ok = distance != n.left.meters()
		case LTE:
			match.Ok = distance <= n.left.meters()
		case GTE:
			match.Ok = distance >= n.left.meters()
		}
		if match.Ok {
			if match.Right.Refs == nil {
				match.Right.Refs = make([]string, 0, len(n.right.Ref))
			}
			match.Right.Refs = append(match.Right.Refs, objectID)
		}
	}
	if match.Ok {
		match.Left.Refs = []string{d.IMEI}
	}
	return
}

type equalDevicesOp struct {
	op    Token
	left  *DeviceLit
	right *DevicesLit
	pos   Pos
}

func (n equalDevicesOp) refIDs() (refs map[string]Token) {
	if n.right != nil && len(n.right.Ref) > 0 {
		refs = make(map[string]Token)
		for i := 0; i < len(n.right.Ref); i++ {
			refs[n.right.Ref[i]] = n.right.Kind
		}
	}
	return
}

func (n equalDevicesOp) evaluate(ctx context.Context, d *Device, _ *State, ref reference) (match Match, err error) {
	match.Left.Keyword = DEVICE
	match.Right.Keyword = DEVICES
	match.Pos = n.pos
	match.Operator = n.op
	for i := 0; i < len(n.right.Ref); i++ {
		deviceID := n.right.Ref[i]
		other, err := ref.devices.Lookup(ctx, deviceID)
		if err != nil {
			if errors.Is(err, ErrObjectNotFound) {
				continue
			}
			return match, err
		}
		distance := round(geo.DistanceTo(
			d.Latitude,
			d.Longitude,
			other.Latitude,
			other.Longitude), minDistMeters)
		switch n.op {
		case EQ:
			match.Ok = distance == n.left.meters()
		case LT:
			match.Ok = distance < n.left.meters()
		case GT:
			match.Ok = distance > n.left.meters()
		case NE:
			match.Ok = distance != n.left.meters()
		case LTE:
			match.Ok = distance <= n.left.meters()
		case GTE:
			match.Ok = distance >= n.left.meters()
		}
		if match.Ok {
			if match.Right.Refs == nil {
				match.Right.Refs = make([]string, 0, len(n.right.Ref))
			}
			match.Right.Refs = append(match.Right.Refs, deviceID)
		}
	}
	if match.Ok {
		match.Left.Refs = []string{d.IMEI}
	}
	return
}

type equalTimeOp struct {
	keyword Token
	op      Token
	value   timeVal
	pos     Pos
}

func (n equalTimeOp) refIDs() (refs map[string]Token) { return }

func (n equalTimeOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	values := mapper{device: d}
	ts := values.dateTime()
	right := time.Date(ts.Year(), ts.Month(), ts.Day(), n.value.h, n.value.m, 0, 0, ts.Location())
	switch n.op {
	case EQ:
		match.Ok = ts.Unix() == right.Unix()
	case LT:
		match.Ok = ts.Unix() < right.Unix()
	case GT:
		match.Ok = ts.Unix() > right.Unix()
	case NE:
		match.Ok = ts.Unix() != right.Unix()
	case LTE:
		match.Ok = ts.Unix() <= right.Unix()
	case GTE:
		match.Ok = ts.Unix() >= right.Unix()
	}
	match.Left.Keyword = n.keyword
	match.Right.Keyword = TIME
	match.Pos = n.pos
	match.Operator = n.op
	return
}

type equalStrOp struct {
	keyword Token
	value   string
	op      Token
	pos     Pos
}

func (n equalStrOp) refIDs() (refs map[string]Token) { return }

func (n equalStrOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	values := mapper{device: d}
	switch n.op {
	case EQ:
		match.Ok = values.stringVal(n.keyword) == n.value
	case LT:
		match.Ok = values.stringVal(n.keyword) < n.value
	case GT:
		match.Ok = values.stringVal(n.keyword) > n.value
	case NE:
		match.Ok = values.stringVal(n.keyword) != n.value
	case LTE:
		match.Ok = values.stringVal(n.keyword) <= n.value
	case GTE:
		match.Ok = values.stringVal(n.keyword) >= n.value
	}
	match.Left.Keyword = n.keyword
	match.Right.Keyword = STRING
	match.Pos = n.pos
	match.Operator = n.op
	return
}

type equalIntOp struct {
	keyword Token
	value   int
	op      Token
	pos     Pos
}

func (n equalIntOp) refIDs() (refs map[string]Token) { return }

func (n equalIntOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	values := mapper{device: d}
	switch n.op {
	case EQ:
		match.Ok = values.intVal(n.keyword) == n.value
	case LT:
		match.Ok = values.intVal(n.keyword) < n.value
	case GT:
		match.Ok = values.intVal(n.keyword) > n.value
	case NE:
		match.Ok = values.intVal(n.keyword) != n.value
	case LTE:
		match.Ok = values.intVal(n.keyword) <= n.value
	case GTE:
		match.Ok = values.intVal(n.keyword) >= n.value
	}
	match.Left.Keyword = n.keyword
	match.Right.Keyword = INT
	match.Pos = n.pos
	match.Operator = n.op
	return
}

type equalFloatOp struct {
	keyword Token
	value   float64
	op      Token
	pos     Pos
}

func (n equalFloatOp) refIDs() (refs map[string]Token) { return }

func (n equalFloatOp) evaluate(_ context.Context, d *Device, _ *State, _ reference) (match Match, err error) {
	values := mapper{device: d}
	switch n.op {
	case EQ:
		match.Ok = values.floatVal(n.keyword) == n.value
	case LT:
		match.Ok = values.floatVal(n.keyword) < n.value
	case GT:
		match.Ok = values.floatVal(n.keyword) > n.value
	case NE:
		match.Ok = values.floatVal(n.keyword) != n.value
	case LTE:
		match.Ok = values.floatVal(n.keyword) <= n.value
	case GTE:
		match.Ok = values.floatVal(n.keyword) >= n.value
	}
	match.Left.Keyword = n.keyword
	match.Right.Keyword = FLOAT
	match.Pos = n.pos
	match.Operator = n.op
	return
}

type radiusRing struct {
	rect   geometry.Rect
	points []geometry.Point
}

func makeRadiusRing(lat, lng float64, meters float64, steps int) radiusRing {
	rr := radiusRing{}
	rr.points, rr.rect = makeCircle(lat, lng, meters, steps)
	return rr
}

func (rr radiusRing) Index() interface{} {
	return nil
}

func (rr radiusRing) Clockwise() bool {
	return true
}

func (rr radiusRing) Move(_, _ float64) geometry.Series {
	return rr
}

func (rr radiusRing) Empty() bool {
	return false
}

func (rr radiusRing) Valid() bool {
	return true
}

func (rr radiusRing) Rect() geometry.Rect {
	return rr.rect
}

func (rr radiusRing) Convex() bool {
	return true
}

func (rr radiusRing) Closed() bool {
	return true
}

func (rr radiusRing) NumPoints() int {
	return len(rr.points)
}

func (rr radiusRing) PointAt(index int) geometry.Point {
	return rr.points[index]
}

func (rr radiusRing) Search(rect geometry.Rect, iter func(seg geometry.Segment, idx int) bool) {
	n := rr.NumSegments()
	for i := 0; i < n; i++ {
		seg := rr.SegmentAt(i)
		if seg.Rect().IntersectsRect(rect) {
			if !iter(seg, i) {
				return
			}
		}
	}
}

func (rr radiusRing) NumSegments() int {
	if len(rr.points) < 3 {
		return 0
	}
	if rr.points[len(rr.points)-1] == rr.points[0] {
		return len(rr.points) - 1
	}
	return len(rr.points)
}

func (rr radiusRing) SegmentAt(index int) geometry.Segment {
	var seg geometry.Segment
	seg.A = rr.points[index]
	if index == len(rr.points)-1 {
		seg.B = rr.points[0]
	} else {
		seg.B = rr.points[index+1]
	}
	return seg
}

func hasMatches(m []Match) bool {
	return len(m) > 0
}

type InvalidExprError struct {
	Left  Expr
	Right Expr
	Op    Token
	Msg   string
	Pos   Pos
}

func (e *InvalidExprError) Error() string {
	return fmt.Sprintf("spinix/runtime: invalid expression: %s %s %s, %s, pos=%d",
		e.Left, e.Op, e.Right, e.Msg, e.Pos+1)
}

type mapper struct {
	device *Device
}

func (m mapper) dateTime() time.Time {
	return time.Unix(m.device.DateTime, 0)
}

func (m mapper) stringVal(keyword Token) (v string) {
	switch keyword {
	case MODEL:
		v = m.device.Model
	case BRAND:
		v = m.device.Brand
	case OWNER:
		v = m.device.Owner
	case IMEI:
		v = m.device.IMEI
	case MONTH:
		dt := time.Unix(m.device.DateTime, 0)
		v = dt.Month().String()
	case DAY:
		dt := time.Unix(m.device.DateTime, 0)
		v = dt.Weekday().String()
	case DATE:
		dt := time.Unix(m.device.DateTime, 0)
		v = dt.Format("2006-01-02")
	case DATETIME:
		dt := time.Unix(m.device.DateTime, 0)
		v = dt.Format(time.RFC3339)
	}
	return v
}

func (m mapper) floatVal(keyword Token) (v float64) {
	switch keyword {
	case FUELLEVEL:
		v = m.device.FuelLevel
	case PRESSURE:
		v = m.device.Pressure
	case LUMINOSITY:
		v = m.device.Luminosity
	case HUMIDITY:
		v = m.device.Humidity
	case TEMPERATURE:
		v = m.device.Temperature
	case BATTERY_CHARGE:
		v = m.device.BatteryCharge
	case STATUS:
		v = float64(m.device.Status)
	case SPEED:
		v = m.device.Speed
	case YEAR:
		dt := time.Unix(m.device.DateTime, 0)
		v = float64(dt.Year())
	case MONTH:
		dt := time.Unix(m.device.DateTime, 0)
		v = float64(dt.Month())
	case WEEK:
		_, week := time.Unix(m.device.DateTime, 0).ISOWeek()
		v = float64(week)
	case DAY:
		dt := time.Unix(m.device.DateTime, 0)
		v = float64(dt.Day())
	case HOUR:
		dt := time.Unix(m.device.DateTime, 0)
		v = float64(dt.Hour())
	}
	return
}

func (m mapper) intVal(keyword Token) (v int) {
	switch keyword {
	case FUELLEVEL:
		v = int(m.device.FuelLevel)
	case PRESSURE:
		v = int(m.device.Pressure)
	case LUMINOSITY:
		v = int(m.device.Luminosity)
	case HUMIDITY:
		v = int(m.device.Humidity)
	case TEMPERATURE:
		v = int(m.device.Temperature)
	case BATTERY_CHARGE:
		v = int(m.device.BatteryCharge)
	case STATUS:
		v = m.device.Status
	case SPEED:
		v = int(m.device.Speed)
	case YEAR:
		dt := time.Unix(m.device.DateTime, 0)
		v = dt.Year()
	case MONTH:
		dt := time.Unix(m.device.DateTime, 0)
		v = int(dt.Month())
	case WEEK:
		_, week := time.Unix(m.device.DateTime, 0).ISOWeek()
		v = week
	case DAY:
		dt := time.Unix(m.device.DateTime, 0)
		v = dt.Day()
	case HOUR:
		dt := time.Unix(m.device.DateTime, 0)
		v = dt.Hour()
	}
	return
}

func round(v, unit float64) float64 {
	return math.Round(v/unit) * unit
}

func bucket(s string, numBuckets int) int {
	if numBuckets == 1 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(s))
	return int(hash.Sum64() % uint64(numBuckets))
}
