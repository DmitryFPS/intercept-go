package sqlite

import (
	"testing"
	"time"

	"intercept/internal/application"
)

// Журнал транспорта описывает в том числе посторонние сообщения в группе.
// Его строки не являются принятыми перехватами или выполненными маршрутами.
func TestTransportAuditCannotIncreaseWorkCounters(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	before, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, item := range []application.TransportObservation{
		{
			UpdateID: 1, MessageID: 101, ChatID: -1001, ThreadID: 10,
			SenderBotID: 0, MessageKind: "user_media", Outcome: "ignored_non_transport",
			Reason: "Пользователь отправил изображение", SentAt: now, ObservedAt: now,
		},
		{
			UpdateID: 2, MessageID: 102, ChatID: -1001, ThreadID: 10,
			SenderBotID: 12, SenderBotUsername: "textile_bot", MessageKind: "event_packet",
			Outcome: "rejected_authentication", Reason: "Неверная подпись", SentAt: now, ObservedAt: now,
		},
	} {
		if err := store.RecordTransportObservation(ctx, item); err != nil {
			t.Fatal(err)
		}
	}
	after, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Events != before.Events || after.Pending != before.Pending ||
		after.Delivered != before.Delivered || after.Expired != before.Expired ||
		after.Rejected != before.Rejected {
		t.Fatalf("диагностические записи изменили рабочие счётчики: before=%+v after=%+v", before, after)
	}
}
