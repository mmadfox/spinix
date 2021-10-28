package spinix

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type Parser struct {
	s  *Scanner
	op Token
}

func newParser(spec string) *Parser {
	return &Parser{
		s: NewScanner(strings.NewReader(spec)),
	}
}

func ParseSpec(spec string) (Expr, error) {
	if len(spec) == 0 {
		return nil, fmt.Errorf("spinix/parser: specification not defined")
	}
	return newParser(spec).Parse()
}

func (p *Parser) Parse() (Expr, error) {
	return p.parse()
}

func (p *Parser) parse() (Expr, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	for {
		operator, literal := p.s.Next()
		if operator == ILLEGAL {
			return nil, fmt.Errorf("spinix/parser: ILLEGAL %s", literal)
		}

		// props { ... }
		if operator == LBRACE {
			p.s.Reset()
			return p.parseProps(expr)
		}

		if (!operator.IsOperator() && !operator.IsKeyword()) || operator == EOF {
			p.s.Reset()
			return expr, nil
		}

		p.op = operator

		rhs, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		lhs, ok := expr.(*BinaryExpr)
		if ok && lhs.Op.Precedence() <= operator.Precedence() {
			expr = &BinaryExpr{
				LHS: lhs.LHS,
				RHS: &BinaryExpr{LHS: lhs.RHS, RHS: rhs, Op: operator},
				Op:  lhs.Op,
			}
		} else {
			expr = &BinaryExpr{
				LHS: expr,
				RHS: rhs,
				Op:  operator,
			}
		}
	}
}

func (p *Parser) parseProps(expr Expr) (Expr, error) {
	props, ok := expr.(*PropExpr)
	if !ok {
		props = &PropExpr{
			Expr: expr,
			List: make([]Expr, 0, 2),
		}
	}
	for {
		tok, lit := p.s.Next()
		if tok == LBRACE {
			continue
		}
		if tok == EOF || tok == RBRACE {
			break
		}
		switch tok {
		case EXPIRE:
			prop, err := p.parseExpireProp()
			if err != nil {
				return nil, err
			}
			props.List = append(props.List, prop)
		case RADIUS:
			prop, err := p.parseRadiusProp()
			if err != nil {
				return nil, err
			}
			props.List = append(props.List, prop)
		case CENTER:
			prop, err := p.parseCenterProp()
			if err != nil {
				return nil, err
			}
			props.List = append(props.List, prop)
		case TRIGGER:
			prop, err := p.parseTriggerProp()
			if err != nil {
				return nil, err
			}
			props.List = append(props.List, prop)
		case RESET:
			prop, err := p.parseResetProp()
			if err != nil {
				return nil, err
			}
			props.List = append(props.List, prop)
		default:
			return nil, p.error(tok, lit, "ILLEGAL")
		}
	}
	return props, nil
}

func (p *Parser) parseExpireProp() (Expr, error) {
	dur, err := p.parseTimeDuration()
	if err != nil {
		return nil, p.error(EXPIRE, ":expire", err.Error())
	}
	return &BaseLit{
		Kind: EXPIRE,
		Expr: &DurationLit{
			Kind:  DURATION,
			Value: dur,
			Pos:   p.s.Offset(),
		},
		Pos: p.s.Offset(),
	}, nil
}

func (p *Parser) parseRadiusProp() (Expr, error) {
	dist, err := p.parseDistanceLit()
	if err != nil {
		return nil, err
	}
	return &BaseLit{
		Kind: RADIUS,
		Expr: dist,
		Pos:  p.s.Offset(),
	}, nil
}

func (p *Parser) parseDistanceLit() (Expr, error) {
	tok, valstr := p.s.Next()
	if tok != INT {
		return nil, p.error(tok, valstr, fmt.Sprintf("got %v, expected %v", tok, INT))
	}
	tok, unitstr := p.s.Next()
	if tok != ILLEGAL {
		return nil, p.error(tok, unitstr, fmt.Sprintf("got %v, expected %v", tok, ILLEGAL))
	}
	value, err := strconv.ParseFloat(valstr, 64)
	if err != nil {
		return nil, p.error(tok, valstr, err.Error())
	}
	var unit DistanceUnit
	switch strings.ToLower(unitstr) {
	case "km":
		unit = DistanceKilometers
	case "m":
		unit = DistanceMeters
	default:
		return nil, p.error(tok, unitstr, fmt.Sprintf("got %s, expected [km, m]", unitstr))
	}
	return &DistanceLit{
		Unit:  unit,
		Pos:   p.s.Offset(),
		Value: value,
	}, nil
}

func (p *Parser) parseCenterProp() (Expr, error) {
	var lat, lon string
	for i := 0; i < 2; i++ {
		tok, lit := p.s.Next()
		if tok != FLOAT && tok != SUB {
			return nil, p.error(tok, lit, "ILLEGAL")
		}
		if tok == SUB {
			tok, value := p.s.Next()
			if tok != FLOAT {
				return nil, p.error(tok, lit, "ILLEGAL")
			}
			lit = "-" + value
		}
		switch i {
		case 0:
			lat = lit
		case 1:
			lon = lit
		}
	}
	if len(lat) == 0 || len(lon) == 0 {
		return nil, p.error(CENTER, ":center", "coordinate parsing error")
	}
	latf, err := strconv.ParseFloat(lat, 64)
	if err != nil {
		return nil, p.error(CENTER, ":center", err.Error())
	}
	lonf, err := strconv.ParseFloat(lon, 64)
	if err != nil {
		return nil, p.error(CENTER, ":center", err.Error())
	}
	return &PointLit{
		Lat:  latf,
		Lon:  lonf,
		Pos:  p.s.Offset(),
		Kind: CENTER,
	}, nil
}

func (p *Parser) parseTriggerProp() (Expr, error) {
	tok, lit := p.s.Next()
	triggerExpr := &TriggerLit{}
	switch tok {
	case INT:
		// 25 times interval 10s
		times, err := strconv.Atoi(lit)
		if err != nil {
			return nil, p.error(TRIGGER, lit, err.Error())
		}
		if lit := p.s.NextLit(); lit != "times" {
			return nil, p.error(TRIGGER, lit, fmt.Sprintf("got %s, expected times", lit))
		}
		if lit := p.s.NextLit(); lit != "interval" {
			return nil, p.error(TRIGGER, lit, fmt.Sprintf("got %s, expected interval", lit))
		}
		dur, err := p.parseTimeDuration()
		if err != nil {
			return nil, err
		}
		triggerExpr.Repeat = RepeatTimes
		triggerExpr.Interval = dur
		triggerExpr.Times = times
	case ILLEGAL:
		// every 10s, once
		if lit == "once" {
			triggerExpr.Repeat = RepeatOnce
		}
		if lit == "every" {
			triggerExpr.Repeat = RepeatEvery
			dur, err := p.parseTimeDuration()
			if err != nil {
				return nil, err
			}
			triggerExpr.Value = dur
		}
	default:
		return nil, p.error(tok, lit, "ILLEGAL")
	}

	triggerExpr.Pos = p.s.Offset()

	return triggerExpr, nil
}

func (p *Parser) parseResetProp() (Expr, error) {
	tok := p.s.NextTok()
	if tok != AFTER {
		return nil, p.error(RESET, ":reset", "invalid expr, expected [:reset after 24h]")
	}
	dur, err := p.parseTimeDuration()
	if err != nil {
		return nil, err
	}
	return &ResetLit{
		Kind:  RESET,
		Pos:   p.s.Offset(),
		After: dur,
	}, nil
}

func (p *Parser) parseExpr() (Expr, error) {
	tok, lit := p.s.Next()
	switch tok {
	case LPAREN:
		return p.parseParenExpr()
	case INT:
		return p.parseIntOrTimeLit(lit)
	case FLOAT:
		return p.parseFloatLit(lit)
	case STRING:
		lit = strings.TrimLeft(lit, `"`)
		lit = strings.TrimRight(lit, `"`)
		return &StringLit{Value: lit, Pos: p.s.Offset()}, nil
	case LBRACK:
		return p.parseListOrRangeLit()
	case DEVICE:
		return p.parseDeviceLit()
	case DEVICES:
		return p.parseDevicesLit()
	case OBJECTS, POLY, MULTI_POLY, LINE, MULTI_LINE,
		POINT, MULTI_POINT, RECT, CIRCLE, COLLECTION, FUT_COLLECTION:
		return p.parseObjectLit(tok)
	case FUELLEVEL, PRESSURE, LUMINOSITY, HUMIDITY, TEMPERATURE, BATTERY_CHARGE,
		STATUS, SPEED, MODEL, BRAND, OWNER, IMEI, YEAR, MONTH, WEEK, DAY, HOUR, TIME, DATETIME, DATE:
		return &IdentLit{Name: lit, Pos: p.s.Offset(), Kind: tok}, nil
	default:
		return nil, p.error(tok, lit, "ILLEGAL")
	}
}

func (p *Parser) parseParenExpr() (Expr, error) {
	expr, err := p.parse()
	if err != nil {
		return nil, err
	}
	if tok, _ := p.s.Next(); tok != RPAREN {
		return nil, fmt.Errorf("spinix/parser: missing )")
	}
	return &ParenExpr{Expr: expr}, nil
}

func (p *Parser) parseDevicesLit() (Expr, error) {
	expr, err := p.parseObjectLit(DEVICES)
	if err != nil {
		return nil, err
	}
	object := expr.(*ObjectLit)
	devices := &DevicesLit{}
	// for all devices
	if len(object.Ref) == 0 {
		devices.All = true
	} else {
		devices.Ref = make([]string, len(object.Ref))
		copy(devices.Ref, object.Ref)
	}
	tok := p.s.NextTok()
	switch tok {
	case BBOX:
		devices.Kind = BBOX
	case RADIUS:
		devices.Kind = RADIUS
	default:
		devices.Pos = p.s.Offset()
		p.s.Reset()
		return devices, nil
	}
	devices.Unit, devices.Value, err = p.parseDistanceUnit()
	if err != nil {
		return nil, err
	}
	devices.Pos = p.s.Offset()
	return devices, nil
}

func (p *Parser) parseListOrRangeLit() (Expr, error) {
	list := &ListLit{Items: make([]Expr, 0, 2)}
	for i := 0; i < math.MaxInt16; i++ {
		tok, lit := p.s.Next()
		// ]
		if tok == RBRACK {
			// list
			if len(list.Items) == 0 {
				return nil, p.error(ILLEGAL, "[]", "expected one or more value")
			}
			// range
			if list.Kind == RANGE && len(list.Items) != 2 {
				return nil, p.error(list.Kind, lit, "missing start or end value")
			}
			list.Pos = p.s.Offset()
			return list, nil
		}
		// [1..
		if tok == PERIOD && (i <= 0 || i > 2) {
			return nil, p.error(list.Kind, "...", "expected [start .. end] ")
		}
		switch tok {
		case INT:
			// int
			if list.Typ == 0 {
				list.Typ = INT
			} else if list.Typ != INT && list.Typ != TIME {
				return nil, p.error(tok, lit, fmt.Sprintf("expected %v literal", list.Typ))
			}

			val, err := p.parseIntOrTimeLit(lit)
			if err != nil {
				return nil, err
			}

			switch n := val.(type) {
			case *IntLit:
				list.Typ = INT
				list.Items = append(list.Items, n)
			case *TimeLit:
				list.Typ = TIME
				list.Items = append(list.Items, n)
			}
		case FLOAT:
			if list.Typ == 0 {
				list.Typ = FLOAT
			} else if list.Typ != FLOAT {
				return nil, p.error(tok, lit, fmt.Sprintf("expected %v literal", list.Typ))
			}
			floatLit, err := p.parseFloatLit(lit)
			if err != nil {
				return nil, err
			}
			list.Items = append(list.Items, floatLit)
		case STRING, ILLEGAL:
			if list.Typ == 0 {
				list.Typ = STRING
			} else if list.Typ != STRING {
				return nil, p.error(tok, lit, fmt.Sprintf("expected %v literal", list.Typ))
			}
			lit = strings.TrimLeft(lit, `"`)
			lit = strings.TrimRight(lit, `"`)
			list.Items = append(list.Items, &StringLit{Value: lit})
		case COMMA:
		case PERIOD:
			list.Kind = RANGE
		}
	}
	return list, nil
}

func (p *Parser) parseObjectLit(kind Token) (expr Expr, err error) {
	lparen, _ := p.s.Next()
	if lparen != LPAREN {
		return nil, p.error(kind, "", "missing (")
	}

	var (
		lastTok Token
		unique  map[string]struct{}
	)

	obj := &ObjectLit{
		Kind: kind,
		Ref:  make([]string, 0, 1),
	}

	badToken := func(tok Token) bool {
		return tok == EOF ||
			(tok != RPAREN && tok != VAR_IDENT &&
				tok != COMMA && tok != IDENT && tok != INT && tok != FLOAT && tok != STRING)
	}

	for {
		tok, lit := p.s.Next()
		if tok == ILLEGAL {
			tok = IDENT
		}
		if badToken(tok) {
			return nil, p.error(tok, lit, "args error")
		}
		isAllowToken := tok == IDENT || tok == STRING || tok == INT || tok == FLOAT
		if isAllowToken && lastTok != VAR_IDENT {
			return nil, p.error(tok, lit, "missing token")
		}
		// )
		if tok == RPAREN {
			if len(obj.Ref) == 0 && kind != DEVICES {
				return nil, p.error(tok, lit, "arguments not found")
			}

			tok = p.s.NextTok()
			if tok != COLON {
				obj.Pos = p.s.Offset()
				p.s.Reset()
				return obj, nil
			}

			// time duration
			tok = p.s.NextTok()
			if tok != TIME {
				obj.Pos = p.s.Offset()
				p.s.Reset()
				return obj, nil
			}
			obj.DurTyp, obj.DurVal, err = p.parseTimeDur()
			if err != nil {
				return nil, err
			}
			obj.Pos = p.s.Offset()
			return obj, nil
		}
		// ident
		lastTok = tok
		if tok == IDENT || tok == INT || tok == FLOAT || tok == STRING {
			if len(lit) > 64 {
				return nil, p.error(tok, lit, "literal too long")
			}
			if unique == nil {
				unique = make(map[string]struct{})
			}
			_, found := unique[lit]
			if found {
				continue
			}
			unique[lit] = struct{}{}
			obj.Ref = append(obj.Ref, lit)
		}
	}
}

func (p *Parser) parseDeviceLit() (expr Expr, err error) {
	device := &DeviceLit{}
	tok := p.s.NextTok()
	switch tok {
	case BBOX:
		device.Kind = BBOX
	case RADIUS:
		device.Kind = RADIUS
	default:
		device.Kind = DEVICE
		device.Pos = p.s.Offset()
		p.s.Reset()
		return device, nil
	}
	device.Unit, device.Value, err = p.parseDistanceUnit()
	if err != nil {
		return
	}
	device.Pos = p.s.Offset()
	return device, err
}

func (p *Parser) parseTimeDuration() (time.Duration, error) {
	var str string
exit:
	for {
		tok, lit := p.s.Next()
		if tok == EOF {
			break exit
		}
		switch tok {
		case ILLEGAL:
			str += lit
			break exit
		case INT:
			str += lit
		}
	}
	return time.ParseDuration(str)
}

func (p *Parser) parseTimeDur() (k Token, dur time.Duration, err error) {
	tok, lit := p.s.Next()
	switch tok {
	case DURATION:
		k = DURATION
	case AFTER:
		k = AFTER
	default:
		err = p.error(tok, lit, "missing duration literal")
		return
	}
	var str string
exit:
	for {
		tok, lit := p.s.Next()
		if tok == EOF {
			break exit
		}
		switch tok {
		case ILLEGAL:
			str += lit
			break exit
		case INT:
			str += lit
		}
	}
	dur, err = time.ParseDuration(str)
	return
}

func (p *Parser) parseDistanceUnit() (u DistanceUnit, r float64, err error) {
	tok, lit := p.s.Next()
	switch tok {
	case FLOAT:
		r, err = strconv.ParseFloat(lit, 64)
	case INT:
		v, err := strconv.Atoi(lit)
		if err != nil {
			return u, r, err
		}
		r = float64(v)
	}
	if err != nil {
		return u, r, err
	}
	if r < 0 {
		return u, r, p.error(tok, lit, "negative distance")
	}
	lit = p.s.NextLit()
	switch strings.ToLower(lit) {
	case "m":
		u = DistanceMeters
	case "km":
		u = DistanceKilometers
	default:
		return u, r, p.error(tok, lit, "missing distance unit")
	}
	return
}

func (p *Parser) parseIntOrTimeLit(val string) (Expr, error) {
	v, err := strconv.Atoi(val)
	if err != nil {
		return nil, p.error(INT, val, err.Error())
	}
	tok := p.s.NextTok()
	if tok != COLON {
		p.s.Reset()
		return &IntLit{Value: v, Pos: p.s.Offset()}, nil
	}
	tok, lit := p.s.Next()
	if tok != INT {
		return nil, p.error(tok, lit, "missing INT literal")
	}
	m, err := strconv.Atoi(lit)
	if err != nil {
		return nil, p.error(INT, val, err.Error())
	}
	return &TimeLit{
		Hour:   v,
		Minute: m,
		Pos:    p.s.Offset(),
	}, nil
}

func (p *Parser) parseFloatLit(val string) (Expr, error) {
	v, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, p.error(FLOAT, val, err.Error())
	}
	return &FloatLit{Value: v, Pos: p.s.Offset()}, nil
}

func (p *Parser) error(tok Token, lit string, msg string) error {
	return &ParserError{
		Pos: p.s.Offset(),
		Lit: lit,
		Tok: tok,
		Msg: msg,
	}
}

type ParserError struct {
	Tok Token
	Lit string
	Pos Pos
	Msg string
}

func (e *ParserError) Error() string {
	return fmt.Sprintf("spinix/parser: parsing error got=%v, lit=%v, pos=%d %s",
		e.Tok, e.Lit, e.Pos, e.Msg)
}
