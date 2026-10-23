// Команда storage-diagnostic проверяет локальную SQLite-базу рабочей точки
// тем же кодом, которым её открывает приложение, не выводя содержимое событий.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/user"
	"time"

	stateDB "intercept/internal/infrastructure/sqlite"
)

type result struct {
	CheckedAt  string `json:"checked_at"`
	User       string `json:"user"`
	Database   string `json:"database"`
	Opened     bool   `json:"opened"`
	QueueReady bool   `json:"queue_ready"`
	Error      string `json:"error,omitempty"`
}

func main() {
	databasePath := flag.String("db", "", "путь к client.db")
	outputPath := flag.String("out", "", "путь к JSON-отчёту")
	flag.Parse()
	if *databasePath == "" || *outputPath == "" {
		fmt.Fprintln(os.Stderr, "обязательны параметры -db и -out")
		os.Exit(2)
	}
	report := result{CheckedAt: time.Now().Format(time.RFC3339Nano), Database: *databasePath}
	if current, err := user.Current(); err == nil {
		report.User = current.Username
	} else {
		report.User = "не определён: " + err.Error()
	}
	database, err := stateDB.OpenState(*databasePath)
	if err != nil {
		report.Error = err.Error()
		writeResult(*outputPath, report)
		return
	}
	report.Opened = true
	defer database.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := stateDB.NewClientQueue(ctx, database); err != nil {
		report.Error = err.Error()
		writeResult(*outputPath, report)
		return
	}
	report.QueueReady = true
	writeResult(*outputPath, report)
}

func writeResult(path string, report result) {
	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
