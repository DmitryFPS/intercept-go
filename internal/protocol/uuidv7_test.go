package protocol

import (
	"bytes"
	"testing"
	"time"
)

func TestUUIDv7ContainsTimestampVersionAndVariant(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 18, 12, 30, 0, 123000000, time.UTC)
	id, err := UUIDv7(now, bytes.NewReader(make([]byte, 16)))
	if err != nil {
		t.Fatalf("uuid: %v", err)
	}
	if id != "01a14efd-85bb-7000-8000-000000000000" {
		t.Fatalf("неожиданный UUID v7: %s", id)
	}
}
