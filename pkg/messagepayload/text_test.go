package messagepayload

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestValidateTextCountsDecodedUnicodeRunes(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{name: "中文上限", content: strings.Repeat("中", 5000)},
		{name: "中文超限", content: strings.Repeat("中", 5001), wantErr: true},
		{name: "表情上限", content: strings.Repeat("😀", 5000)},
		{name: "表情超限", content: strings.Repeat("😀", 5001), wantErr: true},
		{name: "混合上限", content: strings.Repeat("中a😀", 1666) + "文a"},
		{name: "混合超限", content: strings.Repeat("中a😀", 1667), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := json.Marshal(map[string]any{"type": 1, "content": tt.content})
			if err != nil {
				t.Fatal(err)
			}
			before := append([]byte(nil), payload...)
			if err = ValidateText(payload); errors.Is(err, ErrTextTooLong) != tt.wantErr {
				t.Fatalf("ValidateText error = %v, want too long %v", err, tt.wantErr)
			}
			if !bytes.Equal(before, payload) {
				t.Fatal("长度校验不能修改或截断正文")
			}
		})
	}
	if err := ValidateText([]byte(`{"type":1,"content":"` + strings.Repeat(`\u4e2d`, 5000) + `"}`)); err != nil {
		t.Fatalf("合法转义正文被按原始字节数误拒绝: %v", err)
	}
}

func TestValidateTextDoesNotChangeOtherPayloadContracts(t *testing.T) {
	longContent := strings.Repeat("文", 5001)
	for _, contentType := range []int{2, 3, 4, 9, 10, 99, 1000} {
		t.Run(fmt.Sprint(contentType), func(t *testing.T) {
			payload, _ := json.Marshal(map[string]any{"type": contentType, "content": longContent})
			if err := ValidateText(payload); err != nil {
				t.Fatalf("非普通文本被套用了长度限制: %v", err)
			}
		})
	}
	for _, payload := range []string{`plain text`, `{"type":1,"content":123}`, `{"type":1}`, `{"type":1,"content":"` + longContent} {
		if err := ValidateText([]byte(payload)); err != nil {
			t.Fatalf("改变了原有非确定文本/畸形消息合同: %v", err)
		}
	}
}

func TestValidateEventTextChecksCompleteSnapshotOnly(t *testing.T) {
	for _, count := range []int{5000, 5001} {
		for _, nested := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/嵌套%v", count, nested), func(t *testing.T) {
				body := map[string]any{"kind": "text", "text": strings.Repeat("😀", count)}
				if nested {
					body = map[string]any{"snapshot": body}
				}
				payload, _ := json.Marshal(body)
				if err := ValidateEventText(payload); errors.Is(err, ErrTextTooLong) != (count > 5000) {
					t.Fatalf("快照 %d 字符校验错误 = %v", count, err)
				}
			})
		}
	}
	for _, kind := range []string{"image", "video", "file", "tool_call"} {
		payload, _ := json.Marshal(map[string]any{"kind": kind, "text": strings.Repeat("文", 5001)})
		if err := ValidateEventText(payload); err != nil {
			t.Fatalf("非文本快照 %s 被错误限制: %v", kind, err)
		}
	}
}
