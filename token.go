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
	IDENT  // FUN_SPEED
	INT    // 12345
	FLOAT  // 123.45
	STRING // "abc"
	literalEnd

	operatorBegin
	AND   //  AND
	OR    //  OR
	IN    // IN
	NOTIN // NOT IN

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
	EREG   // =~
	NEREG  // !~

	LBRACK // [
	LBRACE // {
	COMMA  // ,
	PERIOD // .

	RBRACK // ]
	RBRACE // }
	COLON  // :
	operatorEnd

	keywordBegin
	VAR_IDENT // @ident

	VAR_SPEED  // {device.speed}
	VAR_STATUS // {device.status}

	FUN_SPEED // speed(min, max), speed(max)
	FUN_EMEI  // emei(one, two, three)
	FUN_OWNER // owner(one, two, three)
	FUN_BRAND // brand(one, two, three)

	FUN_WITHIN
	FUN_WITHIN_RECT
	FUN_WITHIN_POINT
	FUN_WITHIN_POLY
	FUN_WITHIN_LINE

	FUN_NOTWITHIN
	FUN_NOTWITHIN_RECT
	FUN_NOTWITHIN_POINT
	FUN_NOTWITHIN_POLY
	FUN_NOTWITHIN_LINE

	FUN_CONTAINS
	FUN_NOTCONTAINS

	FUN_INTERSECTS
	FUN_INTERSECTS_RECT
	FUN_INTERSECTS_POINT
	FUN_INTERSECTS_LINE
	FUN_INTERSECTS_POLY

	FUN_NOTINTERSECTS
	FUN_NOTINTERSECTS_RECT
	FUN_NOTINTERSECTS_POINT
	FUN_NOTINTERSECTS_LINE
	FUN_NOTINTERSECTS_POLY

	FUN_DISTANCE
	FUN_DISTANCE_RECT
	FUN_DISTANCE_POINT
	FUN_DISTANCE_LINE
	FUN_DISTANCE_POLY

	FUN_BATTERY_CHARGE
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

	AND:   "AND",
	OR:    "OR",
	NOT:   "NOT",
	IN:    "IN",
	NOTIN: "NOT IN",

	EQL:    "==",
	LSS:    "<",
	GTR:    ">",
	ASSIGN: "=",

	NEQ:   "!=",
	LEQ:   "<=",
	GEQ:   ">=",
	EREG:  "=~",
	NEREG: "!~",

	LPAREN: "(",
	LBRACK: "[",
	LBRACE: "{",
	COMMA:  ",",
	PERIOD: ".",

	RPAREN: ")",
	RBRACK: "]",
	RBRACE: "}",
	COLON:  ":",

	VAR_IDENT: "@",

	VAR_SPEED:  "device.speed",
	VAR_STATUS: "device.status",

	FUN_SPEED:          "speed",
	FUN_EMEI:           "emei",
	FUN_OWNER:          "owner",
	FUN_BRAND:          "brand",
	FUN_BATTERY_CHARGE: "batteryCharge",

	FUN_CONTAINS:    "contains",
	FUN_NOTCONTAINS: "not contains",

	FUN_WITHIN:       "within",
	FUN_WITHIN_LINE:  "withinLine",
	FUN_WITHIN_POINT: "withinPoint",
	FUN_WITHIN_POLY:  "withinPoly",
	FUN_WITHIN_RECT:  "withinRect",

	FUN_NOTWITHIN:       "not within",
	FUN_NOTWITHIN_LINE:  "not withinLine",
	FUN_NOTWITHIN_POINT: "not withinPoint",
	FUN_NOTWITHIN_POLY:  "not withinPoly",
	FUN_NOTWITHIN_RECT:  "not withinRect",

	FUN_INTERSECTS:       "intersects",
	FUN_INTERSECTS_LINE:  "intersectsLine",
	FUN_INTERSECTS_POINT: "intersectsPoint",
	FUN_INTERSECTS_POLY:  "intersectsPoly",
	FUN_INTERSECTS_RECT:  "intersectsRect",

	FUN_NOTINTERSECTS:       "not intersects",
	FUN_NOTINTERSECTS_LINE:  "not intersectsLine",
	FUN_NOTINTERSECTS_POINT: "not intersectsPoint",
	FUN_NOTINTERSECTS_POLY:  "not intersectsPoly",
	FUN_NOTINTERSECTS_RECT:  "not intersectsRect",

	FUN_DISTANCE:       "distance",
	FUN_DISTANCE_LINE:  "distanceLine",
	FUN_DISTANCE_POINT: "distancePoint",
	FUN_DISTANCE_POLY:  "distancePoly",
	FUN_DISTANCE_RECT:  "distanceRect",
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
