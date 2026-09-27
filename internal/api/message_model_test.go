package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMessageSendReqCheckTextRuneLimit(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{name: "中文上限", content: strings.Repeat("文", 5000)},
		{name: "中文超限", content: strings.Repeat("文", 5001), wantErr: true},
		{name: "四字节表情上限", content: strings.Repeat("😀", 5000)},
		{name: "四字节表情超限", content: strings.Repeat("😀", 5001), wantErr: true},
		{name: "混合上限", content: strings.Repeat("中a😀", 1666) + "文a"},
		{name: "混合超限", content: strings.Repeat("中a😀", 1667), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(map[string]any{"type": 1, "content": tt.content})
			if err != nil {
				t.Fatal(err)
			}
			if got := (messageSendReq{Payload: payload}).Check(); (got != nil) != tt.wantErr {
				t.Fatalf("Check error = %v, want error %v", got, tt.wantErr)
			}
		})
	}
}
