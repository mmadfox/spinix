package georule

import "strconv"

type (
	Pos   int
	Token int
)

const (
	ILLEGAL Token = iota
	EOF

	literalBegin
	IDENT  // SPEED
	INT    // 12345
	FLOAT  // 123.45
	STRING // "abc"
	literalEnd

	operatorBegin
	AND //  AND
	OR  //  OR

	ADD // +
	SUB // -
	MUL // *
	QUO // /
	REM // %

	EQL    // ==
	LSS    // <
	GTR    // >
	ASSIGN // =
	NOT    // !
	NEQ    // !=
	LEQ    // <=
	GEQ    // >=

	LBRACK // [
	LBRACE // {
	COMMA  // ,
	PERIOD // .

	RBRACK // ]
	RBRACE // }
	COLON  // :
	operatorEnd

	keywordBegin
	VAR                //
	SPEED              // speed(min, max), speed(max)
	BATTERY_CHARGE     // batteryCharge(min, max), batteryCharge(max)
	INTERSECTS_LINE    // intersectsLine(@lineID), intersectsLine(@lineID1, @lineID2, ...)
	INSIDE_POLYGON     // insidePolygon(@polygonID), insidePolygon(@polygonID1, ...)
	OUTSIDE_POLYGON    // outsidePolygon(@polygonID), outsidePolygon(@polygonID1, ...)
	INTERSECTS_POLYGON // intersectPolygon(@polygonID), intersectPolygon(@polygonID1, ...)
	keywordEnd

	RPAREN // )
	LPAREN // (
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",

	EOF: "EOF",

	IDENT:  "IDENT",
	INT:    "INT",
	FLOAT:  "FLOAT",
	STRING: "STRING",

	ADD: "+",
	SUB: "-",
	MUL: "*",
	QUO: "/",
	REM: "%",

	AND: "AND",
	OR:  "OR",

	EQL:    "==",
	LSS:    "<",
	GTR:    ">",
	ASSIGN: "=",
	NOT:    "!",

	NEQ: "!=",
	LEQ: "<=",
	GEQ: ">=",

	LPAREN: "(",
	LBRACK: "[",
	LBRACE: "{",
	COMMA:  ",",
	PERIOD: ".",

	RPAREN: ")",
	RBRACK: "]",
	RBRACE: "}",
	COLON:  ":",

	VAR: "@",

	SPEED:              "speed",
	BATTERY_CHARGE:     "batteryCharge",
	INTERSECTS_LINE:    "intersectsLine",
	INSIDE_POLYGON:     "insidePolygon",
	OUTSIDE_POLYGON:    "outsidePolygon",
	INTERSECTS_POLYGON: "intersectsPolygon",
}

func (tok Token) IsLiteral() bool {
	return literalBegin < tok && tok < literalEnd
}

func (tok Token) IsOperator() bool {
	return operatorBegin < tok && tok < operatorEnd
}

func (tok Token) IsKeyword() bool {
	return keywordBegin < tok && tok < keywordEnd
}

func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case NEQ:
		return 3
	}
	return 0
}

func (tok Token) String() string {
	s := ""
	if 0 <= tok && tok < Token(len(tokens)) {
		s = tokens[tok]
	}
	if s == "" {
		s = "token(" + strconv.Itoa(int(tok)) + ")"
	}
	return s
}
