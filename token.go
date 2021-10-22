package spinix

import (
	"strconv"
	"strings"
)

type (
	Pos   int
	Token int
)

const (
	numberTokenGroup = 1
	stringTokenGroup = 2
	dateTokenGroup   = 3
	timeTokenGroup   = 4
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

	NOTNEAR          // NOT NEAR
	NOTWITHIN        // NOT WITHIN
	NOTINTERSECTS    // NOT INTERSECTS
	NOTINTERSECTSBOX // NOT INTERSECTSBOX
	NOTCONTAINES     // NOT CONTAINS

	IN     // IN
	NIN    // NOT IN
	RANGE  // RANGE
	NRANGE // NOT RANGE
	NEAR   // NEAR

	EQ  // eq  i.e. ==
	LT  // lt  i.e. <
	GT  // gt  i.e. >
	NE  // ne  i.e. !=
	LTE // lte i.e. <=
	GTE // gte i.e. >=

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

	AND:    "AND",
	OR:     "OR",
	IN:     "IN",
	NIN:    "NOT IN",
	NRANGE: "NOT RANGE",

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

	EQ:  "eq",
	LT:  "lt",
	GT:  "gt",
	NE:  "ne",
	LTE: "lte",
	GTE: "gte",

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

var numberToken = map[Token]struct{}{
	FUELLEVEL:      {},
	PRESSURE:       {},
	LUMINOSITY:     {},
	HUMIDITY:       {},
	TEMPERATURE:    {},
	BATTERY_CHARGE: {},
	STATUS:         {},
	SPEED:          {},
	YEAR:           {},
	MONTH:          {},
	WEEK:           {},
	DAY:            {},
	HOUR:           {},
}

var stringToken = map[Token]struct{}{
	MODEL:    {},
	BRAND:    {},
	OWNER:    {},
	IMEI:     {},
	DATE:     {},
	DATETIME: {},
	MONTH:    {},
	DAY:      {},
}

var dateToken = map[Token]struct{}{
	DATE:     {},
	DATETIME: {},
}

var timeToken = map[Token]struct{}{
	TIME: {},
}

func isNumberToken(op Token) bool {
	_, found := numberToken[op]
	return found
}

func isStringToken(op Token) bool {
	_, found := stringToken[op]
	return found
}

func isDateToken(op Token) bool {
	_, found := dateToken[op]
	return found
}

func isTimeToken(op Token) bool {
	_, found := timeToken[op]
	return found
}

func isOneOf(op Token, tokens ...Token) bool {
	for i := 0; i < len(tokens); i++ {
		if op == tokens[i] {
			return true
		}
	}
	return false
}

func group2str(group int) string {
	var res []string
	switch group {
	case numberTokenGroup:
		res = make([]string, 0, len(numberToken))
		for tok := range numberToken {
			res = append(res, tok.String())
		}
	case stringTokenGroup:
		res = make([]string, 0, len(stringToken))
		for tok := range stringToken {
			res = append(res, tok.String())
		}
	case dateTokenGroup:
		res = make([]string, 0, len(dateToken))
		for tok := range dateToken {
			res = append(res, tok.String())
		}
	case timeTokenGroup:
		res = make([]string, 0, len(timeToken))
		for tok := range timeToken {
			res = append(res, tok.String())
		}
	}
	return strings.Join(res, ",")
}

func tok2Str(tokens ...Token) string {
	res := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		res = append(res, tok.String())
	}
	return strings.Join(res, ",")
}
