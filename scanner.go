package spinix

import (
	"io"
	"strings"
	"text/scanner"
)

type Scanner struct {
	s   scanner.Scanner
	tok rune
	lit string
	pos int
}

func NewScanner(r io.Reader) *Scanner {
	s := &Scanner{s: scanner.Scanner{}}
	s.s.Mode = scanner.ScanIdents | scanner.ScanFloats | scanner.ScanStrings
	s.s.Init(r)
	return s
}

func (s *Scanner) Reset() {
	s.pos = 1
}

func (s *Scanner) Offset() Pos {
	return Pos(s.s.Offset)
}

func (s *Scanner) Scan() (rune, string) {
	if s.pos != 0 {
		s.pos = 0
	} else {
		s.tok, s.lit = s.s.Scan(), s.s.TokenText()
	}
	return s.tok, s.lit
}

func (s *Scanner) NextTok() Token {
	tok, _ := s.Next()
	return tok
}

func (s *Scanner) NextLit() string {
	_, lit := s.Next()
	return lit
}

func (s *Scanner) Next() (tok Token, lit string) {
	st, sl := s.Scan()
	switch st {
	case scanner.EOF:
		tok = EOF
	case '.':
		tok = PERIOD
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
	case '-':
		tok = SUB
	case ']':
		tok = RBRACK
	case '{':
		tok = LBRACE
	case '}':
		tok = RBRACE
	case ':':
		tok = COLON
		st, sl = s.Scan()
		switch strings.ToLower(sl) {
		case "trigger":
			tok = TRIGGER
		case "expire":
			tok = EXPIRE
		case "center":
			tok = CENTER
		case "reset":
			tok = RESET
		case "radius":
			tok = RADIUS
		case "bbox":
			tok = BBOX
		case "layer":
			tok = LAYER
		default:
			s.Reset()
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
			switch strings.ToLower(sl) {
			case "layer":
				tok = LAYER
			case "gte":
				tok = GTE
			case "lte":
				tok = LTE
			case "ne":
				tok = NE
			case "gt":
				tok = GT
			case "lt":
				tok = LT
			case "eq":
				tok = EQ
			case "trigger":
				tok = TRIGGER
			case "center":
				tok = CENTER
			case "expire":
				tok = EXPIRE
			case "reset":
				tok = RESET
			case "date":
				tok = DATE
			case "datetime":
				tok = DATETIME
			case "year":
				tok = YEAR
			case "month":
				tok = MONTH
			case "week":
				tok = WEEK
			case "day":
				tok = DAY
			case "hour":
				tok = HOUR
			case "time":
				tok = TIME
			case "radius":
				tok = RADIUS
			case "bbox":
				tok = BBOX
			case "duration":
				tok = DURATION
			case "after":
				tok = AFTER
			case "fuellevel":
				tok = FUELLEVEL
			case "pressure":
				tok = PRESSURE
			case "luminosity":
				tok = LUMINOSITY
			case "humidity":
				tok = HUMIDITY
			case "temperature":
				tok = TEMPERATURE
			case "battery":
				tok = BATTERY_CHARGE
			case "status":
				tok = STATUS
			case "speed":
				tok = SPEED
			case "model":
				tok = MODEL
			case "brand":
				tok = BRAND
			case "owner":
				tok = OWNER
			case "imei":
				tok = IMEI
			case "device":
				tok = DEVICE
			case "range":
				tok = RANGE
			case "nrange":
				tok = NRANGE
			case "in":
				tok = IN
			case "nin":
				tok = NIN
			case "intersects":
				tok = INTERSECTS
			case "nintersects":
				tok = NINTERSECTS
			case "near", "nearby":
				tok = NEAR
			case "nnear":
				tok = NNEAR
			case "and":
				tok = AND
			case "or":
				tok = OR
			default:
				tok = ILLEGAL
			}
		}
	}
	return tok, sl
}
