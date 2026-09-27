package messagepayload

import (
	"encoding/json"
	"errors"
	"unicode/utf8"
)

// MaxTextRunes 是普通文本解码后的 Unicode 字符数上限，不是 payload 字节上限。
const MaxTextRunes = 5000

var ErrTextTooLong = errors.New("普通文本不能超过5000个字符")

// ValidateText 只校验明确的 type=1 字符串正文，保留其他消息和畸形 payload 的原有处理方式。
func ValidateText(payload []byte) error {
	var body map[string]any
	if json.Unmarshal(payload, &body) != nil {
		return nil
	}
	contentType, ok := body["type"].(float64)
	if !ok || contentType != 1 {
		return nil
	}
	content, ok := body["content"].(string)
	if !ok {
		return nil
	}
	return ValidateTextContent(content)
}

// ValidateTextContent 校验完整正文，不能静默截断用户内容。
func ValidateTextContent(content string) error {
	if utf8.RuneCountInString(content) > MaxTextRunes {
		return ErrTextTooLong
	}
	return nil
}

// ValidateEventText 仅校验文本事件的完整快照，工具、图片和其他非文本快照不受此限制。
func ValidateEventText(payload []byte) error {
	if err := ValidateText(payload); err != nil {
		return err
	}
	var body map[string]any
	if json.Unmarshal(payload, &body) != nil {
		return nil
	}
	if err := validateSnapshotText(body); err != nil {
		return err
	}
	if snapshot, ok := body["snapshot"].(map[string]any); ok {
		return validateSnapshotText(snapshot)
	}
	return nil
}

func validateSnapshotText(snapshot map[string]any) error {
	kind, _ := snapshot["kind"].(string)
	if kind != "text" {
		return nil
	}
	text, _ := snapshot["text"].(string)
	return ValidateTextContent(text)
}
