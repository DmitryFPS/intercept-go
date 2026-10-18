package protocol

import (
	"bytes"
	"testing"
	"time"

	"intercept/internal/domain"
)

func TestEnvelopeRoundTripAndTamperProtection(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x42}, 32)
	now := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	event := domain.Event{
		ID: "0199f72d-6f00-7000-8000-000000000001", ClientID: "textile",
		CreatedAt: now, ObservedAt: now,
		Source:   domain.SourceRecord{ID: 7, UUID: "source-7"},
		Position: domain.Position{UnitName: "1 мб 155 омбр"},
		Mode:     domain.SourceModeNormal, Content: "данные", ContentDigest: "abc",
	}
	envelope, err := Seal(event, "key-1", key, now, bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	opened, err := Open(envelope, key)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if opened.Content != event.Content || opened.ID != event.ID {
		t.Fatalf("событие изменилось: %#v", opened)
	}

	envelope.ClientID = "another-client"
	if _, err := Open(envelope, key); err == nil {
		t.Fatal("изменённый конверт не должен проходить проверку")
	}
}
