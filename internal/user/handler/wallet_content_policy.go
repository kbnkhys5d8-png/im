package handler

import (
	"bytes"
	"encoding/json"
	"io"
	"math/big"
	"strings"
)

func reservedWalletContentType(payload []byte) (int, bool) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return 0, false
	}
	reservedType := 0
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return 0, false
		}
		key, ok := keyToken.(string)
		if !ok {
			return 0, false
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return 0, false
		}
		if key != "type" {
			continue
		}
		if contentType, ok := normalizeReservedWalletContentType(raw); ok && reservedType == 0 {
			reservedType = contentType
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return 0, false
	}
	if _, err = decoder.Token(); err != io.EOF {
		return 0, false
	}
	return reservedType, reservedType != 0
}

func normalizeReservedWalletContentType(raw json.RawMessage) (int, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		return 0, false
	}

	var encoded string
	switch typed := value.(type) {
	case json.Number:
		encoded = typed.String()
	case string:
		encoded = strings.TrimSpace(typed)
	default:
		return 0, false
	}
	var number json.Number
	if err := json.Unmarshal([]byte(encoded), &number); err != nil {
		return 0, false
	}
	rational, ok := new(big.Rat).SetString(number.String())
	if !ok {
		return 0, false
	}
	if rational.Cmp(big.NewRat(9, 1)) == 0 {
		return 9, true
	}
	if rational.Cmp(big.NewRat(10, 1)) == 0 {
		return 10, true
	}
	return 0, false
}
