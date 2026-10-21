package sqlite

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ServerStats содержит сводные счётчики без содержимого перехватов.
type ServerStats struct {
	Clients   int `json:"clients"`
	Events    int `json:"events"`
	Pending   int `json:"pending"`
	Delivered int `json:"delivered"`
	Expired   int `json:"expired"`
	Rejected  int `json:"rejected"`
}

// MaintenanceResult описывает только удалённые технические записи.
type MaintenanceResult struct {
	Events   int
	Rejected int
	Rules    int
}

const serverTerminalEventLimit = 200000

// PurgeOld удаляет завершённые задания и старый карантин, не затрагивая
// ожидающие доставки, настройки точек и действующие маршруты.
func (store *ServerStore) PurgeOld(
	ctx context.Context, terminalBefore, rejectedBefore time.Time, keepRuleVersions, keepRejected int,
) (MaintenanceResult, error) {
	if keepRuleVersions < 1 || keepRejected < 1 {
		return MaintenanceResult{}, errors.New("лимиты обслуживания должны быть положительными")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return MaintenanceResult{}, fmt.Errorf("начать обслуживание сервера: %w", err)
	}
	defer tx.Rollback()
	var result MaintenanceResult
	events, err := tx.ExecContext(ctx, `
		DELETE FROM received_events
		WHERE NOT EXISTS (
			SELECT 1 FROM delivery_jobs
			WHERE delivery_jobs.event_id=received_events.event_id
			  AND delivery_jobs.state='pending'
		  )
		  AND (received_at < ? OR event_id NOT IN (
			SELECT candidate.event_id FROM received_events candidate
			WHERE NOT EXISTS (
				SELECT 1 FROM delivery_jobs jobs
				WHERE jobs.event_id=candidate.event_id AND jobs.state='pending'
			)
			ORDER BY candidate.received_at DESC, candidate.event_id DESC
			LIMIT ?
		  ))`, terminalBefore.UTC().Format(time.RFC3339Nano), serverTerminalEventLimit)
	if err != nil {
		return MaintenanceResult{}, fmt.Errorf("очистить завершённые доставки: %w", err)
	}
	if count, countErr := events.RowsAffected(); countErr == nil {
		result.Events = int(count)
	}
	rejected, err := tx.ExecContext(ctx, `
		DELETE FROM rejected_packets
		WHERE received_at < ?
		   OR id NOT IN (SELECT id FROM rejected_packets ORDER BY id DESC LIMIT ?)`,
		rejectedBefore.UTC().Format(time.RFC3339Nano), keepRejected)
	if err != nil {
		return MaintenanceResult{}, fmt.Errorf("очистить старый карантин: %w", err)
	}
	if count, countErr := rejected.RowsAffected(); countErr == nil {
		result.Rejected = int(count)
	}
	rules, err := tx.ExecContext(ctx, `
		DELETE FROM route_versions
		WHERE active=0
		  AND version NOT IN (
			SELECT version FROM route_versions WHERE active=0
			ORDER BY version DESC LIMIT ?
		  )`, keepRuleVersions)
	if err != nil {
		return MaintenanceResult{}, fmt.Errorf("ограничить историю маршрутов: %w", err)
	}
	if count, countErr := rules.RowsAffected(); countErr == nil {
		result.Rules = int(count)
	}
	if err := tx.Commit(); err != nil {
		return MaintenanceResult{}, fmt.Errorf("завершить обслуживание сервера: %w", err)
	}
	return result, nil
}

// Compact возвращает SQLite свободные страницы и ограничивает WAL-файл.
func (store *ServerStore) Compact(ctx context.Context) error {
	return compact(ctx, store.db)
}

// Stats считает состояния сервера без чтения содержимого перехватов.
func (store *ServerStore) Stats(ctx context.Context) (ServerStats, error) {
	var stats ServerStats
	queries := []struct {
		query  string
		target *int
	}{
		{"SELECT COUNT(*) FROM server_clients WHERE active=1", &stats.Clients},
		{"SELECT COUNT(*) FROM received_events", &stats.Events},
		{"SELECT COUNT(*) FROM delivery_jobs WHERE state='pending'", &stats.Pending},
		{"SELECT COUNT(*) FROM delivery_jobs WHERE state='delivered'", &stats.Delivered},
		{"SELECT COUNT(*) FROM delivery_jobs WHERE state='expired'", &stats.Expired},
		{"SELECT COUNT(*) FROM rejected_packets", &stats.Rejected},
	}
	for _, item := range queries {
		if err := store.db.QueryRowContext(ctx, item.query).Scan(item.target); err != nil {
			return ServerStats{}, err
		}
	}
	return stats, nil
}
