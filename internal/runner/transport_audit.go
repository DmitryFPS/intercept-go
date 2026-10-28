package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	transportAuditBufferSize      = 512
	transportAuditWriteLimit      = 2 * time.Second
	transportAuditSpoolFiles      = 10_000
	transportAuditSpoolBytes      = 64 << 20
	transportAuditSpoolEntryBytes = 64 << 10
)

type transportObservationRecorder interface {
	RecordTransportObservation(context.Context, application.TransportObservation) error
}

// transportAuditSink отделяет диагностические записи от приёма и доставки.
// Память ограничена каналом pending, а редкий переполняющий его всплеск
// сохраняется на диск и не теряется. При остановке канал дренируется, а файлы
// удаляются только после подтверждённой записи в базу.
type transportAuditSink struct {
	recorder           transportObservationRecorder
	logger             *slog.Logger
	spoolDir           string
	pending            chan application.TransportObservation
	done               chan struct{}
	close              sync.Once
	gate               sync.RWMutex
	closed             bool
	spoolMu            sync.Mutex
	maxSpoolFiles      int
	maxSpoolBytes      int64
	maxSpoolEntryBytes int64
}

func newTransportAuditSink(
	recorder transportObservationRecorder, logger *slog.Logger, spoolDir string, capacity int,
) *transportAuditSink {
	return &transportAuditSink{
		recorder:           recorder,
		logger:             logger,
		spoolDir:           spoolDir,
		pending:            make(chan application.TransportObservation, capacity),
		done:               make(chan struct{}),
		maxSpoolFiles:      transportAuditSpoolFiles,
		maxSpoolBytes:      transportAuditSpoolBytes,
		maxSpoolEntryBytes: transportAuditSpoolEntryBytes,
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
	sink.gate.RLock()
	if sink.closed {
		sink.gate.RUnlock()
		sink.warn("наблюдение транспорта поступило после остановки журнала",
			"transport_message_id", item.MessageID, "outcome", item.Outcome)
		return
	}
	select {
	case sink.pending <- item:
		sink.gate.RUnlock()
		return
	default:
	}
	sink.gate.RUnlock()
	if err := sink.spool(item); err != nil {
		sink.warn("не удалось сохранить переполненный журнал транспорта",
			"transport_message_id", item.MessageID, "outcome", item.Outcome, "error", err)
		return
	}
	sink.warn("всплеск журнала транспорта сохранён в дисковую очередь",
		"transport_message_id", item.MessageID, "outcome", item.Outcome)
}

func (sink *transportAuditSink) shutdown() {
	sink.close.Do(func() {
		sink.gate.Lock()
		sink.closed = true
		close(sink.pending)
		sink.gate.Unlock()
	})
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
	sink.spoolMu.Lock()
	defer sink.spoolMu.Unlock()
	if err := os.MkdirAll(sink.spoolDir, 0o700); err != nil {
		return err
	}
	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if int64(len(payload)) > sink.maxSpoolEntryBytes {
		return fmt.Errorf("запись журнала транспорта превышает предел %d байт", sink.maxSpoolEntryBytes)
	}
	if err := sink.ensureSpoolCapacity(int64(len(payload))); err != nil {
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

func (sink *transportAuditSink) ensureSpoolCapacity(incoming int64) error {
	entries, err := os.ReadDir(sink.spoolDir)
	if err != nil {
		return err
	}
	count := 0
	var total int64
	for _, entry := range entries {
		if entry.IsDir() || !transportAuditSpoolName(entry.Name(), true) {
			continue
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return infoErr
		}
		if !info.Mode().IsRegular() {
			continue
		}
		count++
		total += info.Size()
	}
	if count >= sink.maxSpoolFiles || total+incoming > sink.maxSpoolBytes {
		return fmt.Errorf("дисковая очередь журнала транспорта достигла предела: файлов=%d, байт=%d", count, total)
	}
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
		if !entry.IsDir() && transportAuditSpoolName(name, includeTemporary) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		path := filepath.Join(sink.spoolDir, name)
		payload, readErr := readTransportAuditSpool(path, sink.maxSpoolEntryBytes)
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

func transportAuditSpoolName(name string, includeTemporary bool) bool {
	if strings.HasPrefix(name, "transport-audit-") && strings.HasSuffix(name, ".json") {
		return true
	}
	return includeTemporary && strings.HasPrefix(name, ".transport-audit-") && strings.HasSuffix(name, ".tmp")
}

func readTransportAuditSpool(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(payload)) > limit {
		return nil, errors.New("запись дисковой очереди журнала превышает допустимый размер")
	}
	return payload, nil
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
