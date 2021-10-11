package spinix

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
	AND           //  AND
	OR            //  OR
	IN            // IN
	NOTIN         // NOT IN
	INTERSECTS    // INTERSECTS
	NEARBY        // NEARBY
	ONDISTANCE    // ON DISTANCE 400
	DISTANCETO    // DISTANCE TO
	DURATIONIN    // DURATION IN
	DURATIONNOTIN // DURATION NOT IN
	RANGE         // RANGE

	ADD // +
	SUB // -
	MUL // *
	QUO // /
	REM // %

	EQL   // ==
	LSS   // <
	GTR   // >
	NOT   // !
	NEQ   // !=
	LEQ   // <=
	GEQ   // >=
	EREG  // =~
	NEREG // !~

	LBRACK // [
	LBRACE // {
	COMMA  // ,
	PERIOD // .

	RBRACK // ]
	RBRACE // }
	COLON  // :
	operatorEnd

	keywordBegin
	VAR_IDENT       // @ident
	VAR_SPEED       // {device.speed}
	VAR_STATUS      // {device.status}
	VAR_EMEI        // {device.emei}
	VAR_OWNER       // {device.owner}
	VAR_BRAND       // {device.brand}
	VAR_MODEL       // {device.model}
	VAR_FUELLEVEL   // {device.fuellevel}
	VAR_PRESSURE    // {device.pressure}
	VAR_LUMONOSITY  // {device.luminosity}
	VAR_HUMIDITY    // {device.humidity}
	VAR_TEMPERATURE // {device.temperature}
	VAR_BATTERY     // {device.battery}

	FUN_DEVICE // device(@), device(one, two, "Three")

	keywordGeospatialBegin
	FUN_OBJECT          // object(@id, @id1)
	FUN_POLY            // polygon(@id1, @id2, @id3), poly(@id)
	FUN_MULTI_POLY      // multiPolygon(@id1, @id2)
	FUN_LINE            // line(@id1, @id2)
	FUN_MULTI_LINE      // multiLine(@id1, @id2)
	FUN_POINT           // point(@id)
	FUN_MULTI_POINT     // multiPoint(@id)
	FUN_RECT            // rect(@id)
	FUN_CIRCLE          // circle(@id)
	FUN_GEOM_COLLECTION // collection(@id)
	FUN_FUT_COLLECTION  // featureCollection(@id1, @id2, @id3)
	keywordGeospatialEnd

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

	AND:           "AND",
	OR:            "OR",
	NOT:           "NOT",
	IN:            "IN",
	NOTIN:         "NOT IN",
	INTERSECTS:    "INTERSECTS",
	NEARBY:        "NEARBY",
	ONDISTANCE:    "ON DISTANCE",
	DISTANCETO:    "DISTANCE TO",
	DURATIONIN:    "DURATION IN",
	DURATIONNOTIN: "DURATION NOT IN",
	RANGE:         "RANGE",

	EQL: "==",
	LSS: "<",
	GTR: ">",
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

	VAR_IDENT:       "@",
	VAR_SPEED:       "device.speed",
	VAR_STATUS:      "device.status",
	VAR_EMEI:        "device.emei",
	VAR_OWNER:       "device.owner",
	VAR_BRAND:       "device.brand",
	VAR_MODEL:       "device.model",
	VAR_FUELLEVEL:   "device.fuellevel",
	VAR_PRESSURE:    "device.pressure",
	VAR_LUMONOSITY:  "device.luminosity",
	VAR_HUMIDITY:    "device.humidity",
	VAR_TEMPERATURE: "device.temperature",
	VAR_BATTERY:     "device.battery",

	FUN_DEVICE: "device",

	FUN_OBJECT:          "object",
	FUN_POLY:            "polygon",
	FUN_MULTI_POLY:      "multiPolygon",
	FUN_LINE:            "line",
	FUN_MULTI_LINE:      "multiLine",
	FUN_POINT:           "point",
	FUN_MULTI_POINT:     "multiPoint",
	FUN_RECT:            "rect",
	FUN_CIRCLE:          "circle",
	FUN_GEOM_COLLECTION: "collection",
	FUN_FUT_COLLECTION:  "featureCollection",
}

var keywords map[string]Token

func init() {
	keywords = make(map[string]Token)
	for i := keywordBegin + 1; i < keywordEnd; i++ {
		keywords[tokens[i]] = i
	}
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

func (tok Token) IsSpatialKeyword() bool {
	if !tok.IsKeyword() {
		return false
	}
	return keywordGeospatialBegin < tok && tok < keywordGeospatialEnd
}

func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case NEQ, LEQ, GEQ, EREG, NEREG, EQL, LSS, GTR, ONDISTANCE, NEARBY, DISTANCETO:
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

func LookupKeyword(ident string) (tok Token, found bool) {
	tok, found = keywords[ident]
	return
}
