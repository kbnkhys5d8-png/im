package handler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientMessageContentType(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    int64
		found   bool
	}{
		{name: "numeric red packet", payload: `{"type":9}`, want: 9, found: true},
		{name: "numeric transfer", payload: `{"type":10}`, want: 10, found: true},
		{name: "decimal red packet", payload: `{"type":9.0}`, want: 9, found: true},
		{name: "exponent transfer", payload: `{"type":10e0}`, want: 10, found: true},
		{name: "string red packet", payload: `{"type":"9"}`, want: 9, found: true},
		{name: "string transfer", payload: `{"type":"10"}`, want: 10, found: true},
		{name: "reserved duplicate before ordinary", payload: `{"type":9,"type":1}`},
		{name: "reserved duplicate after ordinary", payload: `{"type":1,"type":10}`},
		{name: "ordinary", payload: `{"type":1}`, want: 1, found: true},
		{name: "ordinary integer decimal", payload: `{"type":1.0}`, want: 1, found: true},
		{name: "ordinary integer exponent", payload: `{"type":10e-1}`, want: 1, found: true},
		{name: "ordinary integer string", payload: `{"type":"1"}`, want: 1, found: true},
		{name: "exponent string with transfer prefix", payload: `{"type":"10e-1"}`},
		{name: "exponent string with red packet prefix", payload: `{"type":"9e1"}`},
		{name: "fraction string with red packet prefix", payload: `{"type":"9.1e1"}`},
		{name: "noncanonical integer string", payload: `{"type":"01"}`},
		{name: "ordinary media with nested wallet type", payload: `{"type":2,"content":{"type":10}}`, want: 2, found: true},
		{name: "custom integer type", payload: `{"type":10086}`, want: 10086, found: true},
		{name: "fractional transfer", payload: `{"type":10.1}`},
		{name: "fractional ordinary", payload: `{"type":1.00000000000000000001}`},
		{name: "negative type", payload: `{"type":-1}`},
		{name: "overflowing type", payload: `{"type":9223372036854775808}`},
		{name: "int32 upper boundary", payload: `{"type":2147483647}`, want: 2147483647, found: true},
		{name: "int32 overflow", payload: `{"type":2147483648}`},
		{name: "int32 transfer alias", payload: `{"type":4294967306}`},
		{name: "int32 red packet alias", payload: `{"type":"4294967305"}`},
		{name: "oversized exponent", payload: `{"type":1e2147483647}`},
		{name: "oversized string exponent", payload: `{"type":"1e2147483647"}`},
		{name: "boolean type", payload: `{"type":true}`},
		{name: "null type", payload: `{"type":null}`},
		{name: "double encoded type", payload: `{"type":"\"1\""}`},
		{name: "nested reserved type", payload: `{"content":{"type":9}}`},
		{name: "non json", payload: `plain text`},
		{name: "truncated after reserved type", payload: `{"type":9`},
		{name: "trailing garbage after reserved type", payload: `{"type":9}suffix`},
		{name: "truncated later field", payload: `{"type":9,"content":`},
		{name: "near red packet number", payload: `{"type":9.0000000000000001}`},
		{name: "hex float string", payload: `{"type":"0x1.2p3"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := clientMessageContentType([]byte(tt.payload))
			require.Equal(t, tt.found, found)
			require.Equal(t, tt.want, got)
		})
	}
}
