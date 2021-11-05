package spinix

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/rs/xid"

	"github.com/tidwall/geojson"

	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/geojson/geometry"
)

const (
	// Minimum distance in meters.
	// Used to round off the distance to avoid noise.
	minDistMeters = 50
	numBucket     = 256
	dateLayout    = "2006-01-02"
)

type evaluater interface {
	refIDs() map[xid.ID]Token
	evaluate(ctx context.Context, device *Device, state *State, ref reference, props *specProps) (Match, error)
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
	Refs    []xid.ID
}

func defaultRefs() reference {
	return reference{
		devices: NewMemoryDevices(),
		objects: NewMemoryObjects(),
		rules:   NewMemoryRules(),
		states:  NewMemoryState(),
	}
}

type specProps struct {
	resetInterval time.Duration
	times         int
	repeat        RepeatMode
	interval      time.Duration
	delay         time.Duration
	center        geometry.Point
	expire        time.Duration
	radius        float64
	layer         LayerID
}

type spec struct {
	nodes      []evaluater
	ops        []Token
	pos        Pos
	isStateful bool
	props      *specProps
}

func (s *spec) normalizeRadius(size RegionSize) {
	if s.props.radius < minDistMeters {
		s.props.radius = minDistMeters
	}
	s.props.radius = normalizeDistance(s.props.radius, size)
}

func (s *spec) validate() error {
	if s.props.center.X == 0 && s.props.center.Y == 0 {
		return fmt.Errorf("spinix/rule: coordinates are not specified")
	}
	return nil
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
	switch s.props.repeat {
	case RepeatTimes, RepeatOnce:
		state.HitIncr()
	}
}

func (s *spec) checkTrigger(state *State, device *Device) bool {
	switch s.props.repeat {
	case RepeatEvery:
		if state.lastSeenTime == 0 {
			return true
		}
		currTime := mapper{device: device}.dateTime()
		dur := currTime.Unix() - state.LastResetTime()
		return dur > int64(s.props.delay.Seconds())
	case RepeatTimes:
		currTime := mapper{device: device}.dateTime()
		dur := currTime.Unix() - state.LastSeenTime()
		if dur < int64(s.props.interval.Seconds()) {
			return false
		}
		return state.Hits() < s.props.times
	case RepeatOnce:
		return state.hits == 0
	}
	return true
}

func (s *spec) evaluate(ctx context.Context, rid RuleID, d *Device, r reference) (matches []Match, ok bool, err error) {
	if d == nil || len(s.nodes) == 0 || s.props.layer != d.Layer {
		return
	}

	var currState *State
	if s.isStateful {
		sid := StateID{did: d.ID, rid: rid}
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

		if currState.NeedReset(s.props.resetInterval) {
			currState.Reset()
			currState.UpdateLastResetTime()
		}

		if ok := s.checkTrigger(currState, d); !ok {
			return nil, false, nil
		}
	}

	if len(s.nodes) == 1 {
		match, err := s.nodes[0].evaluate(ctx, d, currState, r, s.props)
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

		right, err = s.nodes[index].evaluate(ctx, d, currState, r, s.props)
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

func setupProps(sp *specProps, expr *PropExpr) {
	for i := 0; i < len(expr.List); i++ {
		switch prop := expr.List[i].(type) {
		case *IDLit:
			switch prop.Kind {
			case LAYER:
				sp.layer = prop.Value
			}
		case *PointLit:
			switch prop.Kind {
			case CENTER:
				sp.center = geometry.Point{X: prop.Lat, Y: prop.Lon}
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
				sp.radius = distLit.Value
			case EXPIRE:
				durLit, ok := prop.Expr.(*DurationLit)
				if !ok {
					continue
				}
				sp.expire = durLit.Value
			}
		case *ResetLit:
			sp.resetInterval = prop.After
		case *TriggerLit:
			sp.repeat = prop.Repeat
			sp.delay = prop.Value
			sp.times = prop.Times
			sp.interval = prop.Interval
		}
	}
	if sp.resetInterval == 0 {
		sp.resetInterval = 24 * time.Hour
	}
}

func exprToSpec(e Expr) (*spec, error) {
	s := &spec{
		ops:   make([]Token, 0, 2),
		nodes: make([]evaluater, 0, 2),
		props: new(specProps),
	}

	propExpr, ok := e.(*PropExpr)
	if ok {
		s.isStateful = true
		setupProps(s.props, propExpr)
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

	errInvalidSpec := fmt.Errorf("spinix/runtime: invalid specification %s", e)
	if len(s.nodes) == 0 {
		return nil, errInvalidSpec
	}
	if len(s.nodes)-1 != len(s.ops) {
		return nil, errInvalidSpec
	}
	return s, nil
}

func makeOp(left, right Expr, op Token) (evaluater, error) {
	switch op {
	case INTERSECTS:
		return e2sp(left, right, INTERSECTS)
	case NINTERSECTS:
		return e2sp(left, right, NINTERSECTS)
	case NEAR:
		return e2sp(left, right, NEAR)
	case NNEAR:
		return e2sp(left, right, NNEAR)
	case IN:
		return e2in(left, right, false)
	case NIN:
		return e2in(left, right, true)
	case RANGE:
		return e2range(left, right, false)
	case NRANGE:
		return e2range(left, right, true)
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
	return nil, fmt.Errorf("spinix/runtime: illegal expression %v %v %v", left, op, right)
}

func e2sp(left, right Expr, op Token) (evaluater, error) {
	// device -> devices
	// device -> objects(polygon, circle, rect, ...)
	// object -> device
	// devices -> device
	// devices -> devices
	// devices -> objects
	// device  -> objects
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
			xid.Sort(rhs.Ref)
			return spObjectOp{
				left:  lhs,
				right: rhs,
				pos:   rhs.Pos,
				op:    op,
			}, nil
		case *DevicesLit:
			return spDevicesOp{
				left:  lhs,
				right: rhs,
				pos:   rhs.Pos,
				op:    op,
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
		xid.Sort(lhs.Ref)
		switch rhs := right.(type) {
		case *DevicesLit:
			xid.Sort(rhs.Ref)
			return spDevicesObjectOp{
				left:  rhs,
				right: lhs,
				pos:   lhs.Pos,
				op:    op,
			}, nil
		case *DeviceLit:
			return spObjectOp{
				left:  rhs,
				right: lhs,
				pos:   rhs.Pos,
				op:    op,
			}, nil
		}
	case *DevicesLit:
		xid.Sort(lhs.Ref)
		switch rhs := right.(type) {
		case *ObjectLit:
			if rhs.All && lhs.All {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: right,
					Op:    op,
					Pos:   rhs.Pos,
					Msg:   "illegal",
				}
			}
			xid.Sort(rhs.Ref)
			return spDevicesObjectOp{
				left:  lhs,
				right: rhs,
				pos:   rhs.Pos,
				op:    op,
			}, nil
		case *DevicesLit:
			if rhs.All && lhs.All {
				return nil, &InvalidExprError{
					Left:  lhs,
					Right: right,
					Op:    op,
					Pos:   rhs.Pos,
					Msg:   "illegal",
				}
			}
			xid.Sort(rhs.Ref)
			return spDDevicesOp{
				left:  rhs,
				right: lhs,
				pos:   rhs.Pos,
				op:    op,
			}, nil
		case *DeviceLit:
			return spDevicesOp{
				left:  rhs,
				right: lhs,
				pos:   rhs.Pos,
				op:    op,
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
	// devices -> devices
	// device -> devices
	// devices -> objects
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
					pattern = dateLayout
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

type rangeDateTimeOp struct {
	keyword Token
	begin   time.Time
	end     time.Time
	pos     Pos
	not     bool
}

func (n rangeDateTimeOp) refIDs() (refs map[xid.ID]Token) { return }

func (n rangeDateTimeOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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

func (n rangeTimeOp) refIDs() (refs map[xid.ID]Token) { return }

func (n rangeTimeOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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

func (n rangeIntOp) refIDs() (refs map[xid.ID]Token) { return }

func (n rangeIntOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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

func (n rangeFloatOp) refIDs() (refs map[xid.ID]Token) { return }

func (n rangeFloatOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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

func (n inFloatOp) refIDs() (refs map[xid.ID]Token) { return }

func (n inFloatOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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

type spDevicesObjectOp struct {
	left  *DevicesLit
	right *ObjectLit
	pos   Pos
	op    Token
}

func (n spDevicesObjectOp) refIDs() (refs map[xid.ID]Token) {
	if n.right != nil && len(n.right.Ref) > 0 {
		for i := 0; i < len(n.right.Ref); i++ {
			if refs == nil {
				refs = make(map[xid.ID]Token)
			}
			refs[n.right.Ref[i]] = n.right.Kind
		}
	}
	if n.left != nil && len(n.left.Ref) > 0 {
		for i := 0; i < len(n.left.Ref); i++ {
			if refs == nil {
				refs = make(map[xid.ID]Token)
			}
			refs[n.left.Ref[i]] = n.left.Kind
		}
	}
	return
}

func (n spDevicesObjectOp) evaluate(ctx context.Context, d *Device, s *State, ref reference, props *specProps) (match Match, err error) {
	leftOk := refExists(d.ID, n.left.Ref)
	if !leftOk {
		return
	}
	op := spObjectOp{
		left: &DeviceLit{
			Unit:  n.left.Unit,
			Kind:  n.left.Kind,
			Value: n.left.Value,
			Pos:   n.left.Pos,
		},
		right: n.right,
		pos:   n.pos,
		op:    n.op,
	}
	return op.evaluate(ctx, d, s, ref, props)
}

type spObjectOp struct {
	left  *DeviceLit
	right *ObjectLit
	pos   Pos
	op    Token
}

func (n spObjectOp) refIDs() (refs map[xid.ID]Token) {
	if n.right != nil && len(n.right.Ref) > 0 {
		refs = make(map[xid.ID]Token)
		for i := 0; i < len(n.right.Ref); i++ {
			refs[n.right.Ref[i]] = n.right.Kind
		}
	}
	return
}

func (n spObjectOp) forEachOtherObjects(
	ctx context.Context, layer LayerID, ref reference, lat, lon, meters float64, iter ObjectIterFunc,
) error {
	if len(n.right.Ref) == 0 && n.right.All {
		return ref.objects.Near(ctx, layer, lat, lon, meters, iter)
	}

	if len(n.right.Ref) > 0 && !n.right.All {
		for i := 0; i < len(n.right.Ref); i++ {
			otherID := n.right.Ref[i]
			other, err := ref.objects.Lookup(ctx, otherID)
			if err != nil {
				if errors.Is(err, ErrObjectNotFound) {
					continue
				}
				return err
			}
			if other.Layer() != layer {
				continue
			}
			if err := iter(ctx, other); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n spObjectOp) evaluate(ctx context.Context, target *Device, _ *State, ref reference, props *specProps) (match Match, err error) {
	if target.Layer != props.layer {
		return
	}

	// left device
	var (
		targetRadius *geometry.Poly
		targetPoint  geometry.Point
	)

	switch n.left.Kind {
	case RADIUS, BBOX:
		// circle or rect
		ring := makeRadiusRing(target.Latitude, target.Longitude, n.left.meters(), n.left.steps())
		targetRadius = &geometry.Poly{Exterior: ring}
	default:
		// point
		targetPoint = geometry.Point{X: target.Latitude, Y: target.Longitude}
	}

	var matchOk bool
	if err := n.forEachOtherObjects(ctx, props.layer, ref, target.Latitude, target.Longitude, n.left.meters(),
		func(ctx context.Context, o *GeoObject) error {
			matchOk = false
			switch n.left.Kind {
			case RADIUS:
				if targetRadius == nil {
					return nil
				}
				if n.op == INTERSECTS && o.Data().Spatial().IntersectsPoly(targetRadius) {
					matchOk = true
				}
				if n.op == NINTERSECTS && !o.Data().Spatial().IntersectsPoly(targetRadius) {
					matchOk = true
				}
				if n.op == NEAR && o.Data().Spatial().WithinPoly(targetRadius) {
					matchOk = true
				}
				if n.op == NNEAR && !o.Data().Spatial().WithinPoly(targetRadius) {
					matchOk = true
				}
			case BBOX:
				if targetRadius == nil {
					return nil
				}
				if n.op == INTERSECTS && o.Data().Spatial().IntersectsRect(targetRadius.Rect()) {
					matchOk = true
				}
				if n.op == NINTERSECTS && !o.Data().Spatial().IntersectsRect(targetRadius.Rect()) {
					matchOk = true
				}
				if n.op == NEAR && o.Data().Spatial().WithinRect(targetRadius.Rect()) {
					matchOk = true
				}
				if n.op == NNEAR && !o.Data().Spatial().WithinRect(targetRadius.Rect()) {
					matchOk = true
				}
			default:
				if o.Data().Spatial().IntersectsPoint(targetPoint) {
					matchOk = true
				}
			}
			if matchOk {
				match.Ok = matchOk
				if match.Right.Refs == nil {
					match.Right.Refs = make([]xid.ID, 0, len(n.right.Ref))
				}
				match.Right.Refs = append(match.Right.Refs, o.ID())
			}
			return nil
		}); err != nil {
		return match, err
	}
	if match.Ok {
		match.Left.Keyword = DEVICE
		match.Left.Refs = []xid.ID{target.ID}
		match.Operator = n.op
		match.Pos = n.pos
		match.Right.Keyword = n.right.Kind
	}
	return
}

type spDDevicesOp struct {
	left  *DevicesLit
	right *DevicesLit
	pos   Pos
	op    Token
}

func (n spDDevicesOp) refIDs() (refs map[xid.ID]Token) {
	if len(n.right.Ref) > 0 || len(n.left.Ref) > 0 {
		refs = make(map[xid.ID]Token)
		for i := 0; i < len(n.right.Ref); i++ {
			refs[n.right.Ref[i]] = n.right.Kind
		}
		for i := 0; i < len(n.left.Ref); i++ {
			refs[n.left.Ref[i]] = n.left.Kind
		}
	}
	return
}

// devices(my) intersects devices(others) - OK
// devices(others) intersects devices(my) - OK
// devices(my) intersects devices(my) - BAD
// devices(others) intersects devices(others) - BAD
// devices(my) intersects devices(@) - OK
// devices(@) intersects devices(@) - BAD
func (n spDDevicesOp) evaluate(ctx context.Context, device *Device, state *State, ref reference, props *specProps) (match Match, err error) {
	leftOk := refExists(device.ID, n.left.Ref)
	rightOk := refExists(device.ID, n.right.Ref)
	if leftOk && rightOk {
		return
	}
	if !leftOk && !rightOk {
		return
	}
	if n.left.All && n.right.All {
		return
	}
	// devices(@) OP devices(@my) => devices(@my) OP devices(@)
	if n.left.All && rightOk {
		leftOk = true
		rightOk = false
		n.left, n.right = n.right, n.left
	}

	var op spDevicesOp

	// left -> right
	if leftOk {
		op = spDevicesOp{
			left: &DeviceLit{
				Unit:  n.left.Unit,
				Value: n.left.Value,
				Kind:  n.left.Kind,
				Pos:   n.pos,
			},
			right: n.right,
			pos:   n.right.Pos,
			op:    n.op,
		}
	}
	// right -> left
	if rightOk {
		op = spDevicesOp{
			left: &DeviceLit{
				Unit:  n.right.Unit,
				Value: n.right.Value,
				Kind:  n.right.Kind,
				Pos:   n.pos,
			},
			right: n.left,
			pos:   n.right.Pos,
			op:    n.op,
		}
	}
	return op.evaluate(ctx, device, state, ref, props)
}

func refExists(target xid.ID, list []xid.ID) bool {
	index := sort.Search(len(list), func(i int) bool {
		n := list[i].Compare(target)
		return n >= 0
	})
	if index < len(list) && list[index] == target {
		return true
	}
	return false
}

type spDevicesOp struct {
	left  *DeviceLit
	right *DevicesLit
	pos   Pos
	op    Token
}

func (n spDevicesOp) refIDs() (refs map[xid.ID]Token) {
	if n.right != nil && len(n.right.Ref) > 0 {
		refs = make(map[xid.ID]Token)
		for i := 0; i < len(n.right.Ref); i++ {
			refs[n.right.Ref[i]] = n.right.Kind
		}
	}
	return
}

func (n spDevicesOp) forEachOtherDevices(
	ctx context.Context, ref reference, lat, lon, meters float64, iter DeviceIterFunc,
) error {
	if len(n.right.Ref) == 0 && n.right.All {
		return ref.devices.Near(ctx, lat, lon, meters, iter)
	}

	if len(n.right.Ref) > 0 && !n.right.All {
		for _, otherID := range n.right.Ref {
			otherDevice, err := ref.devices.Lookup(ctx, otherID)
			if err != nil {
				if errors.Is(err, ErrDeviceNotFound) {
					continue
				}
				return err
			}
			if err := iter(ctx, otherDevice); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n spDevicesOp) evaluate(ctx context.Context, target *Device, _ *State, ref reference, props *specProps) (match Match, err error) {
	if target.Layer != props.layer {
		return
	}

	// left device
	var (
		targetRadius *geometry.Poly
		targetPoint  geometry.Point
	)

	targetMeters := n.left.meters()
	switch n.left.Kind {
	case RADIUS, BBOX:
		// circle or rect
		ring := makeRadiusRing(target.Latitude, target.Longitude, targetMeters, n.left.steps())
		targetRadius = &geometry.Poly{Exterior: ring}
	default:
		// point
		targetPoint = geometry.Point{X: target.Latitude, Y: target.Longitude}
	}

	// right devices
	var (
		otherRadius *geometry.Poly
		otherPoint  geometry.Point
	)

	otherMeters := n.right.meters()

	if (n.op == NINTERSECTS || n.op == NNEAR) && n.right.All {
		if targetMeters < TinyRegionThreshold {
			targetMeters = TinyRegionThreshold
		}
		if targetMeters > TinyRegionThreshold {
			targetMeters = SmallRegionThreshold
		}
		if targetMeters > SmallRegionThreshold {
			targetMeters = LargeRegionThreshold
		}
	}

	var matchOk bool
	if err := n.forEachOtherDevices(ctx, ref, target.Latitude, target.Longitude, targetMeters,
		func(ctx context.Context, otherDevice *Device) error {
			if target.Layer != otherDevice.Layer {
				return nil
			}
			matchOk = false
			switch n.right.Kind {
			case RADIUS, BBOX:
				// circle
				ring := makeRadiusRing(otherDevice.Latitude, otherDevice.Longitude, otherMeters, n.right.steps())
				otherRadius = &geometry.Poly{Exterior: ring}

				switch n.right.Kind {
				case RADIUS:
					// with targetRadius
					if targetRadius != nil {
						if n.op == INTERSECTS && targetRadius.IntersectsPoly(otherRadius) {
							matchOk = true
						}
						if n.op == NINTERSECTS && !targetRadius.IntersectsPoly(otherRadius) {
							matchOk = true
						}
						if n.op == NEAR && targetRadius.ContainsPoly(otherRadius) {
							matchOk = true
						}
						if n.op == NNEAR && !targetRadius.ContainsPoly(otherRadius) {
							matchOk = true
						}
					}

					// with targetPoint
					if targetRadius == nil {
						if n.op == INTERSECTS && otherRadius.IntersectsPoint(targetPoint) {
							matchOk = true
						}
						if n.op == NINTERSECTS && !otherRadius.IntersectsPoint(targetPoint) {
							matchOk = true
						}
					}

				case BBOX:
					// with targetRadius
					if targetRadius != nil {
						if n.op == INTERSECTS && otherRadius.IntersectsRect(targetRadius.Rect()) {
							matchOk = true
						}
						if n.op == NINTERSECTS && !otherRadius.IntersectsRect(targetRadius.Rect()) {
							matchOk = true
						}
						if n.op == NEAR && targetRadius.ContainsRect(otherRadius.Rect()) {
							matchOk = true
						}
						if n.op == NNEAR && !targetRadius.ContainsRect(otherRadius.Rect()) {
							matchOk = true
						}
					}

					// with targetPoint
					if targetRadius == nil {
						if n.op == INTERSECTS && otherRadius.IntersectsPoint(targetPoint) {
							matchOk = true
						}
						if n.op == NINTERSECTS && !otherRadius.IntersectsPoint(targetPoint) {
							matchOk = true
						}
					}
				}

				if matchOk {
					match.Ok = matchOk
					if match.Right.Refs == nil {
						match.Right.Refs = make([]xid.ID, 0, len(n.right.Ref))
					}
					match.Right.Refs = append(match.Right.Refs, otherDevice.ID)
				}
			default:
				// point
				otherPoint = geometry.Point{X: otherDevice.Latitude, Y: otherDevice.Longitude}

				// with targetRadius
				if targetRadius != nil {
					if n.op == INTERSECTS && otherPoint.IntersectsPoly(targetRadius) {
						matchOk = true
					}
					if n.op == NINTERSECTS && !otherPoint.IntersectsPoly(targetRadius) {
						matchOk = true
					}
					if n.op == NEAR && targetRadius.ContainsPoint(otherPoint) {
						matchOk = true
					}
					if n.op == NNEAR && !targetRadius.ContainsPoint(otherPoint) {
						matchOk = true
					}
				}

				// with targetPoint
				if targetRadius == nil {
					if n.op == INTERSECTS && otherPoint.IntersectsPoint(targetPoint) {
						matchOk = true
					}
					if n.op == NINTERSECTS && !otherPoint.IntersectsPoint(targetPoint) {
						matchOk = true
					}
					if n.op == NEAR && targetPoint.ContainsPoint(otherPoint) {
						matchOk = true
					}
					if n.op == NNEAR && !targetPoint.ContainsPoint(otherPoint) {
						matchOk = true
					}
				}

				if matchOk {
					match.Ok = matchOk
					if match.Right.Refs == nil {
						match.Right.Refs = make([]xid.ID, 0, len(n.right.Ref))
					}
					match.Right.Refs = append(match.Right.Refs, otherDevice.ID)
				}
			}
			return nil
		}); err != nil {
		return match, err
	}

	if match.Ok {
		match.Left.Keyword = DEVICE
		match.Left.Refs = []xid.ID{target.ID}
		match.Operator = n.op
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

func (n inObjectOp) refIDs() (refs map[xid.ID]Token) {
	if n.object != nil && len(n.object.Ref) > 0 {
		refs = make(map[xid.ID]Token)
		for i := 0; i < len(n.object.Ref); i++ {
			refs[n.object.Ref[i]] = n.object.Kind
		}
	}
	return
}

func (n inObjectOp) evaluate(ctx context.Context, d *Device, _ *State, ref reference, _ *specProps) (match Match, err error) {
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
			if ok := object.data.Contains(deviceRadius); ok {
				match.Ok = true
			}
		} else {
			if ok := object.data.Contains(devicePoint); ok {
				match.Ok = true
			}
		}
		if n.not {
			match.Ok = !match.Ok
		}
		if match.Ok {
			if match.Right.Refs == nil {
				match.Right.Refs = make([]xid.ID, 0, len(n.object.Ref))
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

func (n inIntOp) refIDs() (refs map[xid.ID]Token) { return }

func (n inIntOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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

func (n inStringOp) refIDs() (refs map[xid.ID]Token) { return }

func (n inStringOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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

func (n equalObjectOp) refIDs() (refs map[xid.ID]Token) {
	if n.right != nil && len(n.right.Ref) > 0 {
		refs = make(map[xid.ID]Token)
		for i := 0; i < len(n.right.Ref); i++ {
			refs[n.right.Ref[i]] = n.right.Kind
		}
	}
	return
}

func (n equalObjectOp) evaluate(ctx context.Context, d *Device, _ *State, ref reference, _ *specProps) (match Match, err error) {
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
		center := object.data.Center()
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
				match.Right.Refs = make([]xid.ID, 0, len(n.right.Ref))
			}
			match.Right.Refs = append(match.Right.Refs, objectID)
		}
	}
	if match.Ok {
		match.Left.Refs = []xid.ID{d.ID}
	}
	return
}

type equalDevicesOp struct {
	op    Token
	left  *DeviceLit
	right *DevicesLit
	pos   Pos
}

func (n equalDevicesOp) refIDs() (refs map[xid.ID]Token) {
	if n.right != nil && len(n.right.Ref) > 0 {
		refs = make(map[xid.ID]Token)
		for i := 0; i < len(n.right.Ref); i++ {
			refs[n.right.Ref[i]] = n.right.Kind
		}
	}
	return
}

func (n equalDevicesOp) evaluate(ctx context.Context, d *Device, _ *State, ref reference, _ *specProps) (match Match, err error) {
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
				match.Right.Refs = make([]xid.ID, 0, len(n.right.Ref))
			}
			match.Right.Refs = append(match.Right.Refs, deviceID)
		}
	}
	if match.Ok {
		match.Left.Refs = []xid.ID{d.ID}
	}
	return
}

type equalTimeOp struct {
	keyword Token
	op      Token
	value   timeVal
	pos     Pos
}

func (n equalTimeOp) refIDs() (refs map[xid.ID]Token) { return }

func (n equalTimeOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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

func (n equalStrOp) refIDs() (refs map[xid.ID]Token) { return }

func (n equalStrOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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

func (n equalIntOp) refIDs() (refs map[xid.ID]Token) { return }

func (n equalIntOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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

func (n equalFloatOp) refIDs() (refs map[xid.ID]Token) { return }

func (n equalFloatOp) evaluate(_ context.Context, d *Device, _ *State, _ reference, _ *specProps) (match Match, err error) {
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
		v = dt.Format(dateLayout)
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

func bucketFromID(s xid.ID, numBuckets int) int {
	if numBuckets == 1 {
		return 0
	}
	hash := fnv.New64a()
	_, _ = hash.Write(s.Bytes())
	return int(hash.Sum64() % uint64(numBuckets))
}
