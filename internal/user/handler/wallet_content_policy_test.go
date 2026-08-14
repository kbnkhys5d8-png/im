package handler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReservedWalletContentType(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    int
		found   bool
	}{
		{name: "numeric red packet", payload: `{"type":9}`, want: 9, found: true},
		{name: "numeric transfer", payload: `{"type":10}`, want: 10, found: true},
		{name: "decimal red packet", payload: `{"type":9.0}`, want: 9, found: true},
		{name: "exponent transfer", payload: `{"type":10e0}`, want: 10, found: true},
		{name: "string red packet", payload: `{"type":"9"}`, want: 9, found: true},
		{name: "string transfer", payload: `{"type":"10"}`, want: 10, found: true},
		{name: "reserved duplicate before ordinary", payload: `{"type":9,"type":1}`, want: 9, found: true},
		{name: "reserved duplicate after ordinary", payload: `{"type":1,"type":10}`, want: 10, found: true},
		{name: "ordinary", payload: `{"type":1}`},
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
			got, found := reservedWalletContentType([]byte(tt.payload))
			require.Equal(t, tt.found, found)
			require.Equal(t, tt.want, got)
		})
	}
}
