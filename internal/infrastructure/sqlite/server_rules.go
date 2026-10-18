package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"intercept/internal/domain"
)

// RuleVersion — краткая запись журнала публикаций маршрутов.
type RuleVersion struct {
	Version   int64     `json:"version"`
	Author    string    `json:"author"`
	CreatedAt time.Time `json:"created_at"`
	Active    bool      `json:"active"`
}

func (store *ServerStore) ActiveRules(ctx context.Context) (domain.RuleSet, error) {
	var payload []byte
	err := store.db.QueryRowContext(ctx,
		"SELECT payload_json FROM route_versions WHERE active = 1",
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RuleSet{}, errors.New("маршруты ещё не опубликованы")
	}
	if err != nil {
		return domain.RuleSet{}, fmt.Errorf("прочитать маршруты: %w", err)
	}
	var rules domain.RuleSet
	if err := json.Unmarshal(payload, &rules); err != nil {
		return domain.RuleSet{}, errors.New("опубликованные маршруты повреждены")
	}
	if err := rules.Validate(); err != nil {
		return domain.RuleSet{}, fmt.Errorf("опубликованные маршруты некорректны: %w", err)
	}
	return rules, nil
}

func (store *ServerStore) RuleHistory(ctx context.Context) ([]RuleVersion, error) {
	rows, err := store.db.QueryContext(ctx, `
		SELECT version, author, created_at, active
		FROM route_versions ORDER BY version DESC`)
	if err != nil {
		return nil, fmt.Errorf("прочитать историю маршрутов: %w", err)
	}
	defer rows.Close()
	var result []RuleVersion
	for rows.Next() {
		var item RuleVersion
		var createdAt string
		var active int
		if err := rows.Scan(&item.Version, &item.Author, &createdAt, &active); err != nil {
			return nil, fmt.Errorf("прочитать версию маршрутов: %w", err)
		}
		item.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		item.Active = active == 1
		result = append(result, item)
	}
	return result, rows.Err()
}

func (store *ServerStore) RulesByVersion(ctx context.Context, version int64) (domain.RuleSet, error) {
	var payload []byte
	if err := store.db.QueryRowContext(ctx,
		"SELECT payload_json FROM route_versions WHERE version = ?", version,
	).Scan(&payload); errors.Is(err, sql.ErrNoRows) {
		return domain.RuleSet{}, errors.New("версия маршрутов не найдена")
	} else if err != nil {
		return domain.RuleSet{}, fmt.Errorf("прочитать версию маршрутов: %w", err)
	}
	var rules domain.RuleSet
	if err := json.Unmarshal(payload, &rules); err != nil {
		return domain.RuleSet{}, errors.New("версия маршрутов повреждена")
	}
	return rules, nil
}

func (store *ServerStore) NextRuleVersion(ctx context.Context) (int64, error) {
	var version int64
	if err := store.db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(version), 0) + 1 FROM route_versions",
	).Scan(&version); err != nil {
		return 0, fmt.Errorf("определить следующую версию маршрутов: %w", err)
	}
	return version, nil
}

func (store *ServerStore) PublishRules(ctx context.Context, rules domain.RuleSet, author string) error {
	if err := rules.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(rules)
	if err != nil {
		return fmt.Errorf("кодировать маршруты: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать публикацию маршрутов: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE route_versions SET active = 0 WHERE active = 1"); err != nil {
		return fmt.Errorf("отключить предыдущие маршруты: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO route_versions(version, payload_json, author, created_at, active)
		VALUES(?, ?, ?, ?, 1)`, rules.Version, payload, author,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("опубликовать маршруты: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("завершить публикацию маршрутов: %w", err)
	}
	return nil
}
