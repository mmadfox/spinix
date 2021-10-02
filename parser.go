package georule

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

func New(r io.Reader) *Parser {
	p := &Parser{s: scanner.Scanner{}}
	p.s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanStrings
	p.s.Init(r)
	return p
}

func ParseString(rule string) (Expr, error) {
	return New(strings.NewReader(rule)).Parse()
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
		if lhs, ok := expr.(*BinaryExpr); ok && lhs.Op.Precedence() <= operator.Precedence() {
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
		return p.parseArrayExpr()
	case LBRACE:
		return p.parseVarExpr()
	case INT:
		return p.parseIntExpr(lit)
	case FLOAT:
		return p.parseFloatExpr(lit)
	case STRING:
		return p.parseStringExpr(lit)
	case FUN_EMEI, FUN_OWNER, FUN_BRAND:
		return p.parseCallExprWithArgs(tok)
	case FUN_BATTERY_CHARGE, FUN_SPEED:
		return p.parseCallExprWithRangeArgs(tok)
	case FUN_INTERSECTS, FUN_INTERSECTS_LINE, FUN_INTERSECTS_POINT, FUN_INTERSECTS_POLY, FUN_INTERSECTS_RECT,
		FUN_DISTANCE, FUN_DISTANCE_LINE, FUN_DISTANCE_POINT, FUN_DISTANCE_POLY, FUN_DISTANCE_RECT,
		FUN_NOTINTERSECTS, FUN_NOTINTERSECTS_LINE, FUN_NOTINTERSECTS_POINT, FUN_NOTINTERSECTS_POLY, FUN_NOTINTERSECTS_RECT,
		FUN_WITHIN, FUN_WITHIN_LINE, FUN_WITHIN_POINT, FUN_WITHIN_POLY, FUN_WITHIN_RECT,
		FUN_NOTWITHIN, FUN_NOTWITHIN_LINE, FUN_NOTWITHIN_POINT, FUN_NOTWITHIN_POLY, FUN_NOTWITHIN_RECT,
		FUN_CONTAINS, FUN_NOTCONTAINS:
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

func (p *Parser) parseArrayExpr() (Expr, error) {
	list := &ListLit{}
	for i := 0; i < math.MaxInt16; i++ {
		tok, lit := p.next()
		if tok == EOF || tok == RBRACK {
			if len(list.Items) == 0 {
				return nil, fmt.Errorf("georule/parser: parsing error: tok=%v, lit=%v", tok, lit)
			}
			return list, nil
		}
		switch tok {
		case STRING, ILLEGAL:
			expr, err := p.parseStringExpr(lit)
			if err != nil {
				return nil, err
			}
			list.Items = append(list.Items, expr)
		case INT:
			expr, err := p.parseIntExpr(lit)
			if err != nil {
				return nil, err
			}
			list.Items = append(list.Items, expr)
		case FLOAT:
			expr, err := p.parseFloatExpr(lit)
			if err != nil {
				return nil, err
			}
			list.Items = append(list.Items, expr)
		case COMMA:
		default:
			return nil, fmt.Errorf("georule/parser: parseArrayExpr() parsing error: tok=%v, lit=%v", tok, lit)
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

	for {
		tok, lit := p.next()
		if tok == ILLEGAL {
			tok = IDENT
		}
		if tok == EOF || (tok != RPAREN && tok != COMMA && tok != IDENT && tok != STRING) {
			return nil, fmt.Errorf("georule/parser: %s args error tok=%s, lit=%s",
				keyword, tok, lit)
		}
		if tok == IDENT && prev != COMMA && prev != ILLEGAL {
			return nil, fmt.Errorf("georule/parser: %s args error missed %s token",
				keyword, COMMA)
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
		if tok == IDENT || tok == STRING {
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
				Value: fmt.Sprintf("%s%s", VAR_IDENT, lit),
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
			case "IN":
				tok = IN
			case "AND":
				tok = AND
			case "OR":
				tok = OR
			case "NOT":
				_, not := p.scan()
				switch not {
				case "in", "IN":
					tok = NOTIN
				case "contains":
					tok = FUN_NOTCONTAINS
				case "within":
					tok = FUN_NOTWITHIN
				case "withinLine":
					tok = FUN_NOTWITHIN_LINE
				case "withinPoint":
					tok = FUN_NOTWITHIN_POINT
				case "withinPoly":
					tok = FUN_NOTWITHIN_POLY
				case "withinRect":
					tok = FUN_NOTWITHIN_RECT
				case "intersects":
					tok = FUN_NOTINTERSECTS
				case "intersectsLine":
					tok = FUN_NOTINTERSECTS_LINE
				case "intersectsPoint":
					tok = FUN_NOTINTERSECTS_POINT
				case "intersectsPoly":
					tok = FUN_NOTINTERSECTS_POLY
				case "intersectsRect":
					tok = FUN_NOTINTERSECTS_RECT
				default:
					p.reset()
					tok = ILLEGAL
				}
			default:
				tok = ILLEGAL
			}
		}
	}
	return tok, sl
}
