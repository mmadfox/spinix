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
	IDENT          // operation
	INT            // 12345
	FLOAT          // 123.45
	STRING         // "abc"
	DEVICE         // device
	RADIUS         // radius
	DISTANCE       // distance
	BBOX           // bbox
	TIME           // time
	DURATION       // duration
	AFTER          // after
	FUELLEVEL      // fuellevel
	PRESSURE       // pressure
	LUMINOSITY     // luminosity
	HUMIDITY       // humidity
	TEMPERATURE    // temperature
	BATTERY_CHARGE // batteryCharge
	STATUS         // status
	SPEED          // speed
	MODEL          // model
	BRAND          // brand
	OWNER          // owner
	IMEI           // imei
	VAR_IDENT      // @
	YEAR           // year
	MONTH          // month
	WEEK           // week
	DAY            // day
	HOUR           // hour
	DATE           // date
	DATETIME       // dateTime
	TRIGGER        // trigger
	literalEnd

	operatorBegin
	AND //  AND
	OR  //  OR

	precedenceBegin
	IN            // IN
	WITHIN        // WITHIN
	CONTAINS      // CONTAINS
	INTERSECTSBOX // INTERSECTSBOX
	INTERSECTS    // INTERSECTS
	// <---
	DISTANCETO    // DISTANCE TO
	DURATIONIN    // DURATION IN
	DURATIONNOTIN // DURATION NOT IN
	ONDISTANCE    // ON DISTANCE
	// -->
	NOTIN            // NOT IN
	NEAR             // NEAR
	NOTNEAR          // NOT NEAR
	NOTWITHIN        // NOT WITHIN
	NOTINTERSECTS    // NOT INTERSECTS
	NOTINTERSECTSBOX // NOT INTERSECTSBOX
	NOTCONTAINES     // NOT CONTAINS
	RANGE            // range
	ADD              // +
	SUB              // -
	MUL              // *
	QUO              // /
	REM              // %
	EQL              // ==
	LSS              // <
	GTR              // >
	NOT              // !
	NEQ              // !=
	LEQ              // <=
	GEQ              // >=
	EREG             // =~
	NEREG            // !~
	precedenceEnd

	LBRACK // [
	LBRACE // {
	COMMA  // ,
	PERIOD // .

	RBRACK // ]
	RBRACE // }
	COLON  // :
	operatorEnd

	keywordBegin

	// GEOSPATIAL
	keywordGeospatialBegin
	DEVICES        // devices(@id)
	OBJECTS        // object(@id, @id1)
	POLY           // polygon(@id1, @id2, @id3), poly(@id)
	MULTI_POLY     // multiPolygon(@id1, @id2)
	LINE           // line(@id1, @id2)
	MULTI_LINE     // multiLine(@id1, @id2)
	POINT          // point(@id)
	MULTI_POINT    // multiPoint(@id)
	RECT           // rect(@id)
	CIRCLE         // circle(@id)
	COLLECTION     // collection(@id)
	FUT_COLLECTION // featureCollection(@id1, @id2, @id3)
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

	AND:              "AND",
	OR:               "OR",
	NOT:              "NOT",
	IN:               "IN",
	NOTIN:            "NOT IN",
	NOTNEAR:          "NOT NEAR",
	NOTWITHIN:        "NOT WITHIN",
	NOTINTERSECTS:    "NOT INTERSECTS",
	NOTINTERSECTSBOX: "NOT INTERSECTSBOX",
	NOTCONTAINES:     "NOT CONTAINS",

	FUELLEVEL:      "fuelLevel",
	PRESSURE:       "pressure",
	LUMINOSITY:     "luminosity",
	HUMIDITY:       "humidity",
	TEMPERATURE:    "temperature",
	BATTERY_CHARGE: "battery",
	STATUS:         "status",
	SPEED:          "speed",
	MODEL:          "model",
	BRAND:          "brand",
	OWNER:          "owner",
	IMEI:           "imei",

	WITHIN:        "WITHIN",
	CONTAINS:      "CONTAINS",
	INTERSECTS:    "INTERSECTS",
	INTERSECTSBOX: "INTERSECTSBOX",
	NEAR:          "NEAR",
	DISTANCETO:    "DISTANCE TO",
	DURATIONIN:    "DURATION IN",
	DURATIONNOTIN: "DURATION NOT IN",
	RANGE:         "RANGE",
	ONDISTANCE:    "ON DISTANCE",

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

	DEVICE:         "device",
	TRIGGER:        ":trigger",
	VAR_IDENT:      "@",
	DEVICES:        "devices",
	OBJECTS:        "objects",
	POLY:           "polygon",
	MULTI_POLY:     "multiPolygon",
	LINE:           "line",
	MULTI_LINE:     "multiLine",
	POINT:          "point",
	MULTI_POINT:    "multiPoint",
	RECT:           "rect",
	CIRCLE:         "circle",
	COLLECTION:     "collection",
	FUT_COLLECTION: "featureCollection",

	YEAR:     "year",
	MONTH:    "month",
	WEEK:     "week",
	DAY:      "day",
	HOUR:     "hour",
	DATE:     "date",
	DATETIME: "datetime",
	TIME:     "time",
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

func (tok Token) IsGeospatial() bool {
	return keywordGeospatialBegin < tok && tok < keywordGeospatialEnd
}

func (tok Token) Precedence() int {
	if tok == OR {
		return 1
	}
	if tok == AND {
		return 2
	}
	if precedenceBegin < tok && tok < precedenceEnd {
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
