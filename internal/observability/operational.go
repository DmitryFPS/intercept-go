package observability

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

// OperationalRecord is deliberately content-free: it captures enough data to
// analyse a week or a month of stability without storing intercepted messages.
type OperationalRecord struct {
	Time          time.Time `json:"time"`
	Event         string    `json:"event"`
	Component     string    `json:"component"`
	Version       string    `json:"version"`
	UptimeSeconds int64     `json:"uptime_seconds"`
	MemoryBytes   uint64    `json:"memory_bytes"`
	Goroutines    int       `json:"goroutines"`
	DiskFreeBytes uint64    `json:"disk_free_bytes,omitempty"`
	Status        any       `json:"status,omitempty"`
}

// StartOperationalRecorder writes a snapshot immediately, every fifteen
// minutes, and at shutdown. Twelve bounded archives support long observations.
func StartOperationalRecorder(ctx context.Context, baseDir, component, version string, status func() any) <-chan error {
	errorsChannel := make(chan error, 1)
	go func() {
		defer close(errorsChannel)
		writer := &RotatingWriter{Path: filepath.Join(baseDir, "logs", "operations.log"), MaxBytes: 5 * 1024 * 1024, Backups: 12}
		defer writer.Close()
		started := time.Now()
		write := func(event string) {
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			free, diskErr := diskFree(baseDir)
			if diskErr != nil {
				notifyOperationalError(errorsChannel, diskErr)
			}
			record := OperationalRecord{Time: time.Now(), Event: event, Component: component, Version: version, UptimeSeconds: int64(time.Since(started).Seconds()), MemoryBytes: memory.Alloc, Goroutines: runtime.NumGoroutine(), DiskFreeBytes: free}
			if status != nil {
				record.Status = status()
			}
			payload, err := json.Marshal(record)
			if err != nil {
				notifyOperationalError(errorsChannel, err)
				return
			}
			if _, err := writer.Write(append(payload, '\n')); err != nil {
				notifyOperationalError(errorsChannel, err)
			}
		}
		write("started")
		ticker := time.NewTicker(15 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				write("stopped")
				return
			case <-ticker.C:
				write("snapshot")
			}
		}
	}()
	return errorsChannel
}

func notifyOperationalError(target chan<- error, err error) {
	select {
	case target <- err:
	default:
	}
}

// OperationalFiles lists only bounded safe logs used by diagnostic archives.
func OperationalFiles(baseDir string) []string {
	result := make([]string, 0, 16)
	for index := 0; index <= 12; index++ {
		name := "operations.log"
		if index > 0 {
			name += "." + strconv.Itoa(index)
		}
		path := filepath.Join(baseDir, "logs", name)
		if _, err := os.Stat(path); err == nil {
			result = append(result, path)
		}
	}
	for _, name := range []string{"updates.log", "latest-update.json"} {
		path := filepath.Join(baseDir, "logs", name)
		if _, err := os.Stat(path); err == nil {
			result = append(result, path)
		}
	}
	return result
}
