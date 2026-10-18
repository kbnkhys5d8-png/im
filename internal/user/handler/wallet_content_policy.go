package handler

import (
	"bytes"
	"encoding/json"
	"io"
	"math/big"
	"strconv"
	"strings"
)

// clientMessageContentType 只接受唯一、可精确表示的非负整数类型，禁止客户端和服务端产生解析分歧。
func clientMessageContentType(payload []byte) (int64, bool) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return 0, false
	}
	var contentType int64
	found := false
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
		if found {
			return 0, false
		}
		var valid bool
		contentType, valid = normalizeClientMessageContentType(raw)
		if !valid {
			return 0, false
		}
		found = true
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return 0, false
	}
	if _, err = decoder.Token(); err != io.EOF {
		return 0, false
	}
	return contentType, found
}

func normalizeClientMessageContentType(raw json.RawMessage) (int64, bool) {
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
		// 字符串只接受规范十进制整数，禁止科学计数法被客户端按整数前缀解释。
		contentType, err := strconv.ParseInt(encoded, 10, 32)
		return contentType, err == nil && contentType >= 0 && strconv.FormatInt(contentType, 10) == encoded
	default:
		return 0, false
	}
	// 类型不是业务金额；限制表示长度和指数，避免任意精度解析被超大指数耗尽资源。
	if len(encoded) > 256 {
		return 0, false
	}
	if index := strings.IndexAny(encoded, "eE"); index >= 0 {
		exponent, err := strconv.ParseInt(encoded[index+1:], 10, 16)
		if err != nil || exponent < -256 || exponent > 256 {
			return 0, false
		}
	}
	var number json.Number
	if err := json.Unmarshal([]byte(encoded), &number); err != nil || number.String() != encoded {
		return 0, false
	}
	rational, ok := new(big.Rat).SetString(number.String())
	if !ok || !rational.IsInt() || !rational.Num().IsInt64() || rational.Sign() < 0 {
		return 0, false
	}
	// 限定为非负32位整数，避免大类型值被客户端窄化转换成红包或转账类型。
	contentType := rational.Num().Int64()
	if contentType > 1<<31-1 {
		return 0, false
	}
	return contentType, true
}
