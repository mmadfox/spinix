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
	case ':':
		tok = COLON
		st, sl = s.Scan()
		if strings.ToLower(sl) == "trigger" {
			tok = TRIGGER
		} else {
			s.Reset()
		}
	case '>':
		st, sl = s.Scan()
		if st == '=' {
			tok = GEQ
			sl = ">="
		} else {
			tok = GTR
			sl = ">"
			s.Reset()
		}
	case '<':
		st, sl = s.Scan()
		if st == '=' {
			tok = LEQ
			sl = "<="
		} else {
			tok = LSS
			sl = "<"
			s.Reset()
		}
	case '!':
		st, sl = s.Scan()
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
		st, sl = s.Scan()
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
			switch strings.ToLower(sl) {
			case "trigger":
				tok = TRIGGER
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
			case "radius", "distance":
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
				tok = LUMINOSITY
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
			case "in":
				tok = IN
			case "within":
				tok = WITHIN
			case "contains":
				tok = CONTAINS
			case "intersects":
				tok = INTERSECTS
			case "intersectsBox":
				tok = INTERSECTSBOX
			case "near", "nearby":
				tok = NEAR
			case "and":
				tok = AND
			case "or":
				tok = OR
			case "not":
				switch strings.ToLower(s.NextLit()) {
				case "in":
					tok = NOTIN
				case "near":
					tok = NOTNEAR
				case "within":
					tok = NOTWITHIN
				case "intersects":
					tok = NOTINTERSECTS
				case "intersectsBox":
					tok = NOTINTERSECTSBOX
				case "contains":
					tok = NOTCONTAINES
				}
			default:
				tok = ILLEGAL
			}
		}
	}
	return tok, sl
}
