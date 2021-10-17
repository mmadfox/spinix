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

		if operator == TRIGGER {
			p.s.Reset()
			rhs, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			return &SpecExpr{
				Expr:    expr,
				Trigger: rhs,
			}, nil
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

func (p *Parser) parseExpr() (Expr, error) {
	tok, lit := p.s.Next()
	switch tok {
	case TRIGGER:
		return p.parseTriggerLit()
	case INT:
		return p.parseIntOrTimeLit(lit)
	case FLOAT:
		return p.parseFloatLit(lit)
	case STRING:
		return &StringLit{Value: lit}, nil
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
		return nil, p.error(tok, lit, "")
	}
}

func (p *Parser) parseTriggerLit() (Expr, error) {
	tok, lit := p.s.Next()
	lowlit := strings.ToLower(lit)
	if tok == ILLEGAL && lowlit == "every" {
		dur, err := p.parseDur()
		if err != nil {
			return nil, p.error(TRIGGER, "every", err.Error())
		}
		eof := p.s.NextTok()
		if eof != EOF {
			return nil, p.error(TRIGGER, "eof", "literal must be at the end of the spec")
		}
		return &TriggerLit{Repeat: RepeatEvery, Value: dur}, nil
	} else if tok == ILLEGAL && lowlit == "once" {
		return &TriggerLit{Repeat: RepeatOnce}, nil
	} else if tok == INT {
		v, err := strconv.Atoi(lit)
		if err != nil {
			return nil, p.error(TRIGGER, lit, err.Error())
		}
		times := strings.ToLower(p.s.NextLit())
		if times != "times" {
			return nil, p.error(TRIGGER, "times", "missing times literal")
		}
		interval := strings.ToLower(p.s.NextLit())
		if interval != "interval" {
			return nil, p.error(TRIGGER, "interval", "missing interval literal")
		}
		dur, err := p.parseDur()
		if err != nil {
			return nil, p.error(TRIGGER, "times", err.Error())
		}
		eof := p.s.NextTok()
		if eof != EOF {
			return nil, p.error(TRIGGER, "eof", "literal must be at the end of the spec")
		}
		return &TriggerLit{Repeat: RepeatTimes, Times: v, Interval: dur}, nil
	} else {
		return nil, p.error(TRIGGER, lit, "missing trigger literal")
	}
}

func (p *Parser) parseDevicesLit() (Expr, error) {
	expr, err := p.parseObjectLit(DEVICES)
	if err != nil {
		return nil, err
	}
	object := expr.(*ObjectLit)
	devices := &DevicesLit{}
	devices.Ref = make([]string, len(object.Ref))
	copy(devices.Ref, object.Ref)
	tok := p.s.NextTok()
	switch tok {
	case BBOX:
		devices.Kind = BBOX
	case RADIUS, DISTANCE:
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

			intLit, err := p.parseIntOrTimeLit(lit)
			if err != nil {
				return nil, err
			}

			// time
			tok := p.s.NextTok()
			if tok != COLON {
				if list.Typ == TIME {
					return nil, p.error(TIME, "", "missing time literal")
				}
				list.Items = append(list.Items, intLit)
				p.s.Reset()
				continue
			}

			lit = p.s.NextLit()
			intLit2, err := p.parseIntOrTimeLit(lit)
			if err != nil {
				return nil, err
			}
			if len(list.Items) == 1 && list.Typ == INT {
				return nil, p.error(TIME, "", "ILLEGAL type")
			}
			list.Items = append(list.Items, &TimeLit{
				Hour:   intLit.(*IntLit).Value,
				Minute: intLit2.(*IntLit).Value,
			})
			list.Typ = TIME
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
		Ref:  make([]string, 0, 2),
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
			if len(obj.Ref) == 0 {
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
				return nil, p.error(tok, lit, "id too long")
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
	if tok != COLON {
		device.Pos = p.s.Offset()
		expr = device
		p.s.Reset()
		return device, err
	}

	switch p.s.NextTok() {
	case BBOX:
		device.Kind = BBOX
	case RADIUS, DISTANCE:
		device.Kind = RADIUS
	default:
		return nil, p.error(DEVICE, ":", "missing radius literal")
	}
	device.Unit, device.Value, err = p.parseDistanceUnit()
	if err != nil {
		return
	}
	device.Pos = p.s.Offset()
	return device, err
}

func (p *Parser) parseDur() (time.Duration, error) {
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
		return &IntLit{Value: v}, nil
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
	}, nil
}

func (p *Parser) parseFloatLit(val string) (Expr, error) {
	v, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return nil, p.error(FLOAT, val, err.Error())
	}
	return &FloatLit{Value: v}, nil
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
