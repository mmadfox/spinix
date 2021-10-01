package georule

import (
	"fmt"
	"io"
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
	case INTERSECTS_LINE,
		INTERSECTS_POLYGON,
		OUTSIDE_POLYGON,
		INSIDE_POLYGON:
		return p.parseCallExprWithVarsArgs(tok)
	case LPAREN:
		expr, err := p.parse()
		if err != nil {
			return nil, err
		}
		if tok, _ := p.next(); tok != RPAREN {
			return nil, fmt.Errorf("georule/parser: missing )")
		}
		return &ParenExpr{Expr: expr}, nil
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

func (p *Parser) parseCallExprWithRangeArgs(keyword Token) (Expr, error) {
	return nil, nil
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
			(tok != RPAREN && tok != VAR && tok != COMMA && tok != IDENT) {
			return nil, fmt.Errorf("georule/parser: %s args error tok=%s, lit=%s",
				keyword, tok, lit)
		}
		if tok == IDENT && prev != VAR {
			return nil, fmt.Errorf("georule/parser: %s args error missed %s token",
				keyword, VAR)
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
			if unique == nil {
				unique = make(map[string]struct{})
			}
			_, found := unique[lit]
			if found {
				continue
			}
			unique[lit] = struct{}{}
			list = append(list, &BasicLit{
				Kind:  IDENT,
				Value: lit,
			})
		}
	}
}

func (p *Parser) next() (tok Token, lit string) {
	st, sl := p.scan()
	switch st {
	case scanner.EOF:
		tok = EOF
	case '@':
		tok = VAR
	case '(':
		tok = LPAREN
	case ')':
		tok = RPAREN
	case ',':
		tok = COMMA
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
			tu := strings.ToUpper(sl)
			switch tu {
			case "AND":
				tok = AND
			case "OR":
				tok = OR
			default:
				tok = ILLEGAL
			}
		}
	}
	return tok, sl
}
