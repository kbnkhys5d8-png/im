package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEventMarshalUnmarshal(t *testing.T) {
	event := Event{
		Type: 1,
		From: 123,
		To:   456,
		Term: 2,
		Logs: []Log{
			{Index: 1, Term: 1, Data: []byte("log1")},
			{Index: 2, Term: 2, Data: []byte("log2")},
		},
	}

	encoded, err := event.Marshal()
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded Event
	if err := decoded.Unmarshal(encoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if event.Type != decoded.Type || event.From != decoded.From || event.To != decoded.To || event.Term != decoded.Term {
		t.Errorf("Mismatch after Unmarshal: %+v vs %+v", event, decoded)
	}

	if len(event.Logs) != len(decoded.Logs) {
		t.Fatalf("Log length mismatch: %d vs %d", len(event.Logs), len(decoded.Logs))
	}

	for i := range event.Logs {
		assert.Equal(t, event.Logs[i].Index, decoded.Logs[i].Index)
		assert.Equal(t, event.Logs[i].Term, decoded.Logs[i].Term)
		assert.Equal(t, event.Logs[i].Data, decoded.Logs[i].Data)
	}
}

func TestTermResponseEventRoundTrip(t *testing.T) {
	if TermResp != Destory+1 {
		t.Fatalf("TermResp value = %d, want append-only value %d", TermResp, Destory+1)
	}
	event := Event{
		Type: TermResp,
		From: 2,
		To:   1,
		Term: 12,
	}

	encoded, err := event.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	var decoded Event
	if err := decoded.Unmarshal(encoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Type != TermResp || decoded.From != 2 || decoded.To != 1 || decoded.Term != 12 {
		t.Fatalf("decoded term response = %+v, want type:%d from:2 to:1 term:12", decoded, TermResp)
	}
	if got := TermResp.String(); got != "TermResp" {
		t.Fatalf("term response event name = %q, want TermResp", got)
	}
}
