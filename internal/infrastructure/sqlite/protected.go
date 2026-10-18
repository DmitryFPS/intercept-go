package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"intercept/internal/infrastructure/secrets"
)

var protectedPrefix = []byte("INTERCEPT-DPAPI1:")

func protectDatabaseValue(plain []byte) ([]byte, error) {
	protected, err := secrets.Protect(plain)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), protectedPrefix...), protected...), nil
}

func unprotectDatabaseValue(value []byte) ([]byte, error) {
	if !bytes.HasPrefix(value, protectedPrefix) {
		return nil, errorsNewUnprotectedValue()
	}
	plain, err := secrets.Unprotect(value[len(protectedPrefix):])
	if err != nil {
		return nil, fmt.Errorf("расшифровать локальные данные: %w", err)
	}
	return plain, nil
}

func errorsNewUnprotectedValue() error {
	return fmt.Errorf("локальные данные не защищены")
}

// protectExistingColumn выполняет однократную миграцию старой открытой БД.
func protectExistingColumn(ctx context.Context, db *sql.DB, table, column string) error {
	query := fmt.Sprintf("SELECT rowid, %s FROM %s", column, table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("прочитать данные для защиты %s.%s: %w", table, column, err)
	}
	type entry struct {
		rowID   int64
		payload []byte
	}
	var entries []entry
	for rows.Next() {
		var item entry
		if err := rows.Scan(&item.rowID, &item.payload); err != nil {
			rows.Close()
			return err
		}
		if !bytes.HasPrefix(item.payload, protectedPrefix) {
			entries = append(entries, item)
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	update := fmt.Sprintf("UPDATE %s SET %s=? WHERE rowid=?", table, column)
	for _, item := range entries {
		protected, err := protectDatabaseValue(item.payload)
		if err != nil {
			return fmt.Errorf("защитить данные %s.%s: %w", table, column, err)
		}
		if _, err := tx.ExecContext(ctx, update, protected, item.rowID); err != nil {
			return err
		}
	}
	return tx.Commit()
}
