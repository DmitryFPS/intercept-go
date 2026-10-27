package runner

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"intercept/internal/application"
)

const (
	transportAuditBufferSize = 512
	transportAuditWriteLimit = 2 * time.Second
)

type transportObservationRecorder interface {
	RecordTransportObservation(context.Context, application.TransportObservation) error
}

// transportAuditSink отделяет диагностические записи от приёма и доставки.
// Память ограничена каналом pending, а редкий переполняющий его всплеск
// сохраняется на диск и не теряется. При остановке канал дренируется, а файлы
// удаляются только после подтверждённой записи в базу.
type transportAuditSink struct {
	recorder transportObservationRecorder
	logger   *slog.Logger
	spoolDir string
	pending  chan application.TransportObservation
	done     chan struct{}
	close    sync.Once
}

func newTransportAuditSink(
	recorder transportObservationRecorder, logger *slog.Logger, spoolDir string, capacity int,
) *transportAuditSink {
	return &transportAuditSink{
		recorder: recorder,
		logger:   logger,
		spoolDir: spoolDir,
		pending:  make(chan application.TransportObservation, capacity),
		done:     make(chan struct{}),
	}
}

func (sink *transportAuditSink) run() {
	defer close(sink.done)
	sink.flushSpool(true)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case item, ok := <-sink.pending:
			if !ok {
				sink.flushSpool(true)
				return
			}
			sink.recordOrSpool(item)
		case <-ticker.C:
			sink.flushSpool(false)
		}
	}
}

func (sink *transportAuditSink) submit(item application.TransportObservation) {
	select {
	case sink.pending <- item:
		return
	default:
	}
	if err := sink.spool(item); err != nil {
		sink.warn("не удалось сохранить переполненный журнал транспорта",
			"transport_message_id", item.MessageID, "outcome", item.Outcome, "error", err)
		return
	}
	sink.warn("всплеск журнала транспорта сохранён в дисковую очередь",
		"transport_message_id", item.MessageID, "outcome", item.Outcome)
}

func (sink *transportAuditSink) shutdown() {
	sink.close.Do(func() { close(sink.pending) })
	<-sink.done
}

func (sink *transportAuditSink) recordOrSpool(item application.TransportObservation) {
	if err := sink.record(item); err != nil {
		if spoolErr := sink.spool(item); spoolErr != nil {
			sink.warn("не удалось сохранить журнал транспорта ни в базе, ни в дисковой очереди",
				"transport_message_id", item.MessageID, "outcome", item.Outcome,
				"error", err, "spool_error", spoolErr)
			return
		}
		sink.warn("запись журнала транспорта отложена в дисковую очередь",
			"transport_message_id", item.MessageID, "outcome", item.Outcome, "error", err)
		return
	}
	sink.logRecorded(item)
}

func (sink *transportAuditSink) record(item application.TransportObservation) error {
	writeContext, cancel := context.WithTimeout(context.Background(), transportAuditWriteLimit)
	defer cancel()
	return sink.recorder.RecordTransportObservation(writeContext, item)
}

func (sink *transportAuditSink) spool(item application.TransportObservation) error {
	if err := os.MkdirAll(sink.spoolDir, 0o700); err != nil {
		return err
	}
	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(sink.spoolDir, ".transport-audit-*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	completed := false
	defer func() {
		_ = temporary.Close()
		if !completed {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	finalName := filepath.Join(sink.spoolDir,
		"transport-audit-"+strings.TrimSuffix(filepath.Base(temporaryName), ".tmp")+".json")
	if err := os.Rename(temporaryName, finalName); err != nil {
		return err
	}
	completed = true
	return nil
}

func (sink *transportAuditSink) flushSpool(includeTemporary bool) {
	entries, err := os.ReadDir(sink.spoolDir)
	if err != nil {
		if !os.IsNotExist(err) {
			sink.warn("не удалось открыть дисковую очередь журнала транспорта", "error", err)
		}
		return
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		completed := strings.HasPrefix(name, "transport-audit-") && strings.HasSuffix(name, ".json")
		temporary := includeTemporary && strings.HasPrefix(name, ".transport-audit-") && strings.HasSuffix(name, ".tmp")
		if !entry.IsDir() && (completed || temporary) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		path := filepath.Join(sink.spoolDir, name)
		payload, readErr := os.ReadFile(path)
		if readErr != nil {
			sink.warn("не удалось прочитать отложенную запись журнала транспорта", "error", readErr)
			continue
		}
		var item application.TransportObservation
		if decodeErr := json.Unmarshal(payload, &item); decodeErr != nil {
			sink.warn("повреждённая запись журнала транспорта сохранена для диагностики",
				"file", name, "error", decodeErr)
			continue
		}
		if recordErr := sink.record(item); recordErr != nil {
			sink.warn("отложенная запись журнала транспорта ожидает следующей попытки",
				"transport_message_id", item.MessageID, "outcome", item.Outcome, "error", recordErr)
			continue
		}
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			sink.warn("записанный элемент дисковой очереди журнала не удалён",
				"file", name, "error", removeErr)
		}
		sink.logRecorded(item)
	}
}

func (sink *transportAuditSink) logRecorded(item application.TransportObservation) {
	if item.Rank < 100 || sink.logger == nil {
		return
	}
	sink.logger.Info("транспортное сообщение классифицировано",
		"trace_id", item.EventID, "transport_message_id", item.MessageID,
		"outcome", item.Outcome, "counted", item.Counted)
}

func (sink *transportAuditSink) warn(message string, args ...any) {
	if sink.logger != nil {
		sink.logger.Warn(message, args...)
	}
}
