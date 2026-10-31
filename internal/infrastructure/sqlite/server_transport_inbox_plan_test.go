package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPendingTransportPacketsUsesDueExpressionIndex(t *testing.T) {
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := NewServerStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, `
		EXPLAIN QUERY PLAN
		SELECT update_id,chat_id,message_id,packet_json,attempts,decode_failures FROM transport_inbox
		WHERE julianday(next_attempt_at)<=julianday(?)
		ORDER BY julianday(next_attempt_at),update_id LIMIT ?`,
		time.Now().UTC().Format(time.RFC3339Nano), 25)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	usedDueIndex := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "transport_inbox_due_jd_idx") {
			usedDueIndex = true
		}
		if strings.Contains(detail, "USE TEMP B-TREE FOR ORDER BY") {
			t.Fatalf("сортировка входной очереди обходит индекс: %s", detail)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !usedDueIndex {
		t.Fatal("план выборки входной очереди не использует transport_inbox_due_jd_idx")
	}
}
