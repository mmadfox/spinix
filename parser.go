package spinix

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"text/scanner"
)

type Parser struct {
	s   scanner.Scanner
	tok rune
	lit string
	pos Pos
}

func newParser(r io.Reader) *Parser {
	p := &Parser{s: scanner.Scanner{}}
	p.s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanStrings
	p.s.Init(r)
	return p
}

func ParseSpec(spec string) (Expr, error) {
	return newParser(strings.NewReader(spec)).Parse()
}

func (p *Parser) Parse() (Expr, error) {
	return p.parse()
}

func (p *Parser) reset() {
	p.pos = 1
}

func (p *Parser) parse() (Expr, error) {
	expr, err := p.parseExprOrKeyword()
	if err != nil {
		return nil, err
	}

	for {
		operator, literal := p.next()
		if operator == ILLEGAL {
			return nil, fmt.Errorf("georule/parser: ILLEGAL %s", literal)
		}

		if (!operator.IsOperator() && !operator.IsKeyword()) || operator == EOF {
			p.reset()
			return expr, nil
		}

		rhs, err := p.parseExprOrKeyword()
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

func (p *Parser) parseExprOrKeyword() (Expr, error) {
	tok, lit := p.next()
	switch tok {
	case LBRACK:
		return p.parseArrayOrRangeExpr()
	case LBRACE:
		return p.parseVarExpr()
	case INT:
		return p.parseIntExpr(lit)
	case FLOAT:
		return p.parseFloatExpr(lit)
	case STRING:
		return p.parseStringExpr(lit)
	case FUN_DEVICE:
		return p.parseCallExprWithArgs(tok)
	case FUN_POLY, FUN_MULTI_POLY, FUN_POINT, FUN_LINE, FUN_MULTI_LINE, FUN_MULTI_POINT,
		FUN_RECT, FUN_CIRCLE, FUN_GEOM_COLLECTION, FUN_FUT_COLLECTION, FUN_OBJECT:
		return p.parseCallExprWithVarsArgs(tok)
	case LPAREN:
		return p.parseParenExpr()
	default:
		return nil, fmt.Errorf("georule/parser: parsing error: tok=%v, lit=%v", tok, lit)
	}
}

func (p *Parser) scan() (rune, string) {
	if p.pos != 0 {
		p.pos = 0
	} else {
		p.tok, p.lit = p.s.Scan(), p.s.TokenText()
	}
	return p.tok, p.lit
}

func (p *Parser) parseIntExpr(lit string) (Expr, error) {
	v, err := strconv.Atoi(lit)
	if err != nil {
		return nil, fmt.Errorf("georule/parser: parseIntExpr(%s) => %v", lit, err)
	}
	return &IntLit{Value: v}, nil
}

func (p *Parser) parseArrayOrRangeExpr() (Expr, error) {
	list := &ListLit{}
	var (
		isRange  bool
		rangeVal int
	)
	for i := 0; i < math.MaxInt16; i++ {
		tok, lit := p.next()
		if isRange && i > 3 {
			return nil, fmt.Errorf("georule/parser: range parsing error: tok=%v, lit=%v", tok, lit)
		}

		if tok == EOF || tok == RBRACK {
			if len(list.Items) == 0 {
				return nil, fmt.Errorf("georule/parser: parsing error: tok=%v, lit=%v", tok, lit)
			}

			// list literal
			if !isRange {
				return list, nil
			}

			// range literal
			if rangeVal != 2 || len(list.Items) != 2 {
				return nil, fmt.Errorf("georule/parser: range parsing error: tok=%v, lit=%v", tok, lit)
			}

			var (
				rangeFloat *RangeFloatLit
				rangeInt   *RangeIntLit
			)

			// start
			switch typ := list.Items[0].(type) {
			case *IntLit:
				rangeInt = &RangeIntLit{Start: typ.Value}
			case *FloatLit:
				rangeFloat = &RangeFloatLit{Start: typ.Value}
			}

			// end
			switch typ := list.Items[1].(type) {
			default:
				return nil, fmt.Errorf("georule/parser: range parsing error: tok=%v, lit=%v", tok, lit)
			case *IntLit:
				if rangeInt == nil {
					return nil, fmt.Errorf("georule/parser: range parsing error: tok=%v, lit=%v", tok, lit)
				}
				rangeInt.End = typ.Value
				return rangeInt, nil
			case *FloatLit:
				if rangeFloat == nil {
					return nil, fmt.Errorf("georule/parser: range parsing error: tok=%v, lit=%v", tok, lit)
				}
				rangeFloat.End = typ.Value
				return rangeFloat, nil
			}
		}

		switch tok {
		case SUB:
			isRange = true
		case STRING, ILLEGAL:
			expr, err := p.parseStringExpr(lit)
			if err != nil {
				return nil, err
			}
			list.Items = append(list.Items, expr)
		case INT:
			rangeVal++
			expr, err := p.parseIntExpr(lit)
			if err != nil {
				return nil, err
			}
			list.Items = append(list.Items, expr)
		case FLOAT:
			rangeVal++
			expr, err := p.parseFloatExpr(lit)
			if err != nil {
				return nil, err
			}
			list.Items = append(list.Items, expr)
		case COMMA:
		default:
			return nil, fmt.Errorf("georule/parser: parseArrayOrRangeExpr() parsing error: tok=%v, lit=%v", tok, lit)
		}
	}
	return nil, nil
}

func (p *Parser) parseVarExpr() (Expr, error) {
	tok, lit := p.next()
	if lit != "device" {
		return nil, fmt.Errorf("georule/parser: parsing error: tok=%v, lit=%v", tok, lit)
	}
	p.next()
	_, lit = p.next()
	switch strings.ToLower(lit) {
	case "speed":
		tok = VAR_SPEED
	case "status":
		tok = VAR_STATUS
	case "emei":
		tok = VAR_EMEI
	case "owner":
		tok = VAR_OWNER
	case "brand":
		tok = VAR_BRAND
	case "model":
		tok = VAR_MODEL
	case "fuellevel":
		tok = VAR_FUELLEVEL
	case "pressure":
		tok = VAR_PRESSURE
	case "luminosity":
		tok = VAR_LUMONOSITY
	case "humidity":
		tok = VAR_HUMIDITY
	case "temperature":
		tok = VAR_TEMPERATURE
	case "battery":
		tok = VAR_BATTERY
	default:
		return nil, fmt.Errorf("georule/parser: parsing error: tok=%v, lit=%v", tok, lit)
	}
	rbrace, _ := p.next()
	if rbrace != RBRACE {
		return nil, fmt.Errorf("georule/parser: missing }")
	}
	return &VarLit{Value: tok}, nil
}

func (p *Parser) parseFloatExpr(lit string) (Expr, error) {
	v, err := strconv.ParseFloat(lit, 64)
	if err != nil {
		return nil, fmt.Errorf("georule/parser: parseFloatExpr(%s) => %v", lit, err)
	}
	return &FloatLit{Value: v}, nil
}

func (p *Parser) parseStringExpr(lit string) (Expr, error) {
	return &StringLit{Value: lit}, nil
}

func (p *Parser) parseParenExpr() (Expr, error) {
	expr, err := p.parse()
	if err != nil {
		return nil, err
	}
	if tok, _ := p.next(); tok != RPAREN {
		return nil, fmt.Errorf("georule/parser: missing )")
	}
	return &ParenExpr{Expr: expr}, nil
}

func (p *Parser) parseCallExprWithRangeArgs(keyword Token) (Expr, error) {
	lparen, _ := p.next()
	if lparen != LPAREN {
		return nil, fmt.Errorf("georule/parser: %s missed (", keyword)
	}
	var list []Expr
	for {
		tok, lit := p.next()
		if tok == ILLEGAL {
			tok = IDENT
		}
		if tok == EOF ||
			(tok != RPAREN && tok != COMMA && tok != INT && tok != FLOAT) {
			return nil, fmt.Errorf("georule/parser: %s args error tok=%s, lit=%s",
				keyword, tok, lit)
		}
		if tok == INT {
			v, err := strconv.Atoi(lit)
			if err != nil {
				return nil, fmt.Errorf("georule/parser: %s args error tok=%s, lit=%s",
					keyword, tok, lit)
			}
			list = append(list, &IntLit{Value: v})
		}
		if tok == FLOAT {
			v, err := strconv.ParseFloat(lit, 64)
			if err != nil {
				return nil, fmt.Errorf("georule/parser: %s args error tok=%s, lit=%s",
					keyword, tok, lit)
			}
			list = append(list, &FloatLit{Value: v})
		}
		if tok == RPAREN {
			if len(list) == 0 {
				return nil, fmt.Errorf("georule/parser: %s arguments not found", keyword)
			}
			if len(list) > 2 {
				return nil, fmt.Errorf("georule/parser: %s exceeds the number of arguments", keyword)
			}
			return &CallExpr{
				Fun:  keyword,
				Args: list,
			}, nil
		}
	}
}

func (p *Parser) parseCallExprWithArgs(keyword Token) (Expr, error) {
	lparen, _ := p.next()
	if lparen != LPAREN {
		return nil, fmt.Errorf("georule/parser: %s missed (", keyword)
	}

	var (
		prev   Token
		list   []Expr
		unique map[string]struct{}
	)
	var useContext bool
	for {
		tok, lit := p.next()
		if tok == ILLEGAL {
			tok = IDENT
		}
		if tok == EOF || (tok != VAR_IDENT && tok != RPAREN && tok != COMMA && tok != IDENT && tok != STRING) {
			return nil, fmt.Errorf("georule/parser: %s args error tok=%s, lit=%s",
				keyword, tok, lit)
		}
		if tok == IDENT && prev != COMMA && prev != ILLEGAL {
			return nil, fmt.Errorf("georule/parser: %s args error missed %s token",
				keyword, COMMA)
		}
		if tok == RPAREN {
			if len(list) == 0 && !useContext {
				return nil, fmt.Errorf("georule/parser: %s arguments not found", keyword)
			}
			return &CallExpr{
				Fun:    keyword,
				UseCtx: useContext,
				Args:   list,
			}, nil
		}
		prev = tok
		if tok == IDENT || tok == STRING || tok == VAR_IDENT {
			if err := p.validateLen(lit); err != nil {
				return nil, err
			}
			if unique == nil {
				unique = make(map[string]struct{})
			}
			_, found := unique[lit]
			if found {
				continue
			}
			unique[lit] = struct{}{}
			if tok == VAR_IDENT {
				useContext = true
				continue
			}
			list = append(list, &StringLit{
				Value: lit,
			})
		}
	}
}

func (p *Parser) parseCallExprWithVarsArgs(keyword Token) (Expr, error) {
	lparen, _ := p.next()
	if lparen != LPAREN {
		return nil, fmt.Errorf("georule/parser: %s missed (", keyword)
	}

	var (
		prev   Token
		list   []Expr
		unique map[string]struct{}
	)

	for {
		tok, lit := p.next()
		if tok == ILLEGAL {
			tok = IDENT
		}
		if tok == EOF ||
			(tok != RPAREN && tok != VAR_IDENT && tok != COMMA && tok != IDENT) {
			return nil, fmt.Errorf("georule/parser: %s args error tok=%s, lit=%s",
				keyword, tok, lit)
		}
		if tok == IDENT && prev != VAR_IDENT {
			return nil, fmt.Errorf("georule/parser: %s args error missed %s token",
				keyword, VAR_IDENT)
		}
		if tok == RPAREN {
			if len(list) == 0 {
				return nil, fmt.Errorf("georule/parser: %s arguments not found", keyword)
			}
			return &CallExpr{
				Fun:  keyword,
				Args: list,
			}, nil
		}
		prev = tok
		if tok == IDENT {
			if err := p.validateLen(lit); err != nil {
				return nil, err
			}
			if unique == nil {
				unique = make(map[string]struct{})
			}
			_, found := unique[lit]
			if found {
				continue
			}
			unique[lit] = struct{}{}
			list = append(list, &StringLit{
				Value: lit,
			})
		}
	}
}

func (p *Parser) validateLen(lit string) (err error) {
	if len(lit) > 256 {
		err = fmt.Errorf("georule/parser: identificator %s too long", lit)
	}
	return
}

func (p *Parser) next() (tok Token, lit string) {
	st, sl := p.scan()
	switch st {
	case scanner.EOF:
		tok = EOF
	case '@':
		tok = VAR_IDENT
	case '(':
		tok = LPAREN
	case ')':
		tok = RPAREN
	case ',':
		tok = COMMA
	case '[':
		tok = LBRACK
	case ']':
		tok = RBRACK
	case '{':
		tok = LBRACE
	case '}':
		tok = RBRACE
	case '+':
		tok = ADD
	case '-':
		tok = SUB
	case '/':
		tok = QUO
	case '*':
		tok = MUL
	case '%':
		tok = REM
	case '>':
		st, sl = p.scan()
		if st == '=' {
			tok = GEQ
			sl = ">="
		} else {
			tok = GTR
			sl = ">"
			p.reset()
		}
	case '<':
		st, sl = p.scan()
		if st == '=' {
			tok = LEQ
			sl = "<="
		} else {
			tok = LSS
			sl = "<"
			p.reset()
		}
	case '!':
		st, sl = p.scan()
		if st == '=' {
			tok = NEQ
			sl = "!="
		} else if st == '~' {
			tok = NEREG
			sl = "!~"
		} else {
			tok = ILLEGAL
		}
	case '=':
		st, sl = p.scan()
		if st == '=' {
			tok = EQL
			sl = "=="
		} else if st == '~' {
			tok = EREG
			sl = "=~"
		} else {
			tok = ILLEGAL
		}
	case scanner.Float:
		tok = FLOAT
	case scanner.Int:
		tok = INT
	case scanner.String:
		tok = STRING
	case scanner.Ident:
		keyword, found := LookupKeyword(sl)
		if found {
			tok = keyword
		} else {
			switch strings.ToUpper(sl) {
			case "RANGE":
				tok = RANGE
			case "DURATION":
				st, sl = p.scan()
				if sl == "in" {
					tok = DURATIONIN
				} else if sl == "not" {
					st, sl = p.scan()
					if sl == "in" {
						tok = DURATIONNOTIN
					} else {
						tok = ILLEGAL
						p.reset()
					}
				} else {
					tok = ILLEGAL
					p.reset()
				}
			case "DISTANCE":
				st, sl = p.scan()
				if sl == "to" {
					tok = DISTANCETO
				} else {
					tok = ILLEGAL
					p.reset()
				}
			case "ON":
				st, sl = p.scan()
				if sl == "distance" {
					tok = ONDISTANCE
				} else {
					tok = ILLEGAL
					p.reset()
				}
			case "IN":
				tok = IN
			case "INTERSECTS":
				tok = INTERSECTS
			case "NEARBY":
				tok = NEARBY
			case "AND":
				tok = AND
			case "OR":
				tok = OR
			case "NOT":
				// TODO:
			default:
				tok = ILLEGAL
			}
		}
	}
	return tok, sl
}
