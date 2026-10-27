package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"intercept/internal/application"
)

type transportAuditRecorderStub struct {
	mu      sync.Mutex
	items   []application.TransportObservation
	err     error
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (stub *transportAuditRecorderStub) RecordTransportObservation(
	_ context.Context, item application.TransportObservation,
) error {
	if stub.started != nil {
		stub.once.Do(func() { close(stub.started) })
		<-stub.release
	}
	if stub.err != nil {
		return stub.err
	}
	stub.mu.Lock()
	stub.items = append(stub.items, item)
	stub.mu.Unlock()
	return nil
}

func (stub *transportAuditRecorderStub) messageIDs() []int64 {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	result := make([]int64, 0, len(stub.items))
	for _, item := range stub.items {
		result = append(result, item.MessageID)
	}
	return result
}

func TestTransportAuditSinkSpoolsOverflowAndDrainsOnShutdown(t *testing.T) {
	spoolDir := filepath.Join(t.TempDir(), "spool")
	recorder := &transportAuditRecorderStub{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	sink := newTransportAuditSink(recorder, nil, spoolDir, 1)
	go sink.run()

	sink.submit(application.TransportObservation{MessageID: 1})
	<-recorder.started
	sink.submit(application.TransportObservation{MessageID: 2})
	sink.submit(application.TransportObservation{MessageID: 3})

	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		t.Fatalf("read overflow spool: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("overflow spool entries = %d, want 1", len(entries))
	}
	close(recorder.release)
	sink.shutdown()

	got := recorder.messageIDs()
	if len(got) != 3 {
		t.Fatalf("recorded observations = %v, want all three", got)
	}
	seen := map[int64]bool{}
	for _, id := range got {
		seen[id] = true
	}
	for _, id := range []int64{1, 2, 3} {
		if !seen[id] {
			t.Fatalf("message %d was lost; recorded = %v", id, got)
		}
	}
	entries, err = os.ReadDir(spoolDir)
	if err != nil {
		t.Fatalf("read drained spool: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("spool was not drained: %v", entries)
	}
}

func TestTransportAuditSinkShutdownDrainsBufferedItems(t *testing.T) {
	recorder := &transportAuditRecorderStub{}
	sink := newTransportAuditSink(recorder, nil, filepath.Join(t.TempDir(), "spool"), 8)
	for id := int64(1); id <= 8; id++ {
		sink.submit(application.TransportObservation{MessageID: id})
	}
	go sink.run()
	sink.shutdown()

	if got := recorder.messageIDs(); len(got) != 8 {
		t.Fatalf("recorded observations = %v, want 8", got)
	}
}

func TestTransportAuditSinkRecoversDatabaseFailureFromDisk(t *testing.T) {
	spoolDir := filepath.Join(t.TempDir(), "spool")
	failing := &transportAuditRecorderStub{err: errors.New("database unavailable")}
	first := newTransportAuditSink(failing, nil, spoolDir, 1)
	go first.run()
	first.submit(application.TransportObservation{MessageID: 41, Outcome: "event_accepted"})
	first.shutdown()

	entries, err := os.ReadDir(spoolDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("durable retry entry missing: entries=%v err=%v", entries, err)
	}

	recovered := &transportAuditRecorderStub{}
	second := newTransportAuditSink(recovered, nil, spoolDir, 1)
	go second.run()
	second.shutdown()

	got := recovered.messageIDs()
	if len(got) != 1 || got[0] != 41 {
		t.Fatalf("recovered observations = %v, want [41]", got)
	}
	entries, err = os.ReadDir(spoolDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("recovered spool not removed: entries=%v err=%v", entries, err)
	}
}
