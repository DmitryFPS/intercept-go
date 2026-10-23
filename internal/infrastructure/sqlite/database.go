package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// OpenState открывает собственную базу приложения и применяет безопасные pragma.
func OpenState(path string) (*sql.DB, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("получить полный путь базы: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
		return nil, fmt.Errorf("создать папку базы: %w", err)
	}
	dsn := "file:" + filepath.ToSlash(abs) +
		"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(FULL)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("открыть SQLite: %w", err)
	}
	// WAL позволяет коротким рабочим операциям продолжаться, пока локальный
	// интерфейс читает историю. Одно соединение превращало большой поиск в
	// остановку приёма и доставки.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("проверить SQLite: %w", err)
	}
	return db, nil
}

// Migrate выполняет миграции последовательно и в одной транзакции на версию.
func Migrate(ctx context.Context, db *sql.DB, migrations []string) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at TEXT NOT NULL,
			checksum TEXT NOT NULL DEFAULT ''
		)`); err != nil {
		return fmt.Errorf("создать журнал миграций: %w", err)
	}
	rows, err := db.QueryContext(ctx, "PRAGMA table_info(schema_migrations)")
	if err != nil {
		return fmt.Errorf("проверить журнал миграций: %w", err)
	}
	hasChecksum := false
	for rows.Next() {
		var id, notNull, primaryKey int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&id, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		if name == "checksum" {
			hasChecksum = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !hasChecksum {
		if _, err := db.ExecContext(ctx, "ALTER TABLE schema_migrations ADD COLUMN checksum TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("обновить журнал миграций: %w", err)
		}
	}
	var newest int
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version),0) FROM schema_migrations").Scan(&newest); err != nil {
		return err
	}
	if newest > len(migrations) {
		return fmt.Errorf("база создана более новой версией программы (схема %d, поддерживается %d)", newest, len(migrations))
	}

	for index, migration := range migrations {
		version := index + 1
		digest := sha256.Sum256([]byte(migration))
		checksum := hex.EncodeToString(digest[:])
		var stored string
		err := db.QueryRowContext(ctx,
			"SELECT checksum FROM schema_migrations WHERE version = ?", version,
		).Scan(&stored)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("проверить миграцию %d: %w", version, err)
		}
		if err == nil {
			if stored == "" {
				if _, err := db.ExecContext(ctx, "UPDATE schema_migrations SET checksum=? WHERE version=?", checksum, version); err != nil {
					return fmt.Errorf("записать отпечаток миграции %d: %w", version, err)
				}
			} else if stored != checksum {
				return fmt.Errorf("миграция %d изменена после применения; запуск остановлен для защиты данных", version)
			}
			continue
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("начать миграцию %d: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, migration); err != nil {
			tx.Rollback()
			return fmt.Errorf("выполнить миграцию %d: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO schema_migrations(version, applied_at, checksum) VALUES(?, ?, ?)",
			version, time.Now().UTC().Format(time.RFC3339Nano), checksum,
		); err != nil {
			tx.Rollback()
			return fmt.Errorf("зафиксировать миграцию %d: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("завершить миграцию %d: %w", version, err)
		}
	}
	return nil
}

func compact(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return fmt.Errorf("очистить журнал SQLite: %w", err)
	}
	if _, err := db.ExecContext(ctx, "VACUUM"); err != nil {
		return fmt.Errorf("уплотнить SQLite: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		return fmt.Errorf("оптимизировать SQLite: %w", err)
	}
	return nil
}
