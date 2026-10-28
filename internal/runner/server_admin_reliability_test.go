package runner

import (
	"context"
	"errors"
	"testing"

	"intercept/internal/application"
)

func TestPersistReceiptIndependentlyIgnoresCancelledRequestAndRetries(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	attempts := 0
	err := persistReceiptIndependently(func(saveContext context.Context) error {
		attempts++
		if saveContext.Err() != nil {
			t.Fatalf("независимый контекст уже отменён: %v", saveContext.Err())
		}
		if cancelled.Err() == nil {
			t.Fatal("исходный запрос в тесте должен быть отменён")
		}
		if attempts < 3 {
			return errors.New("временная блокировка SQLite")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("результат не сохранён после временной ошибки: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("ожидалось 3 попытки, выполнено %d", attempts)
	}
}

func TestReceiptHasPublishedBlock(t *testing.T) {
	receipt := application.DeliveryReceipt{Blocks: []application.DeliveryBlockReceipt{
		{BlockUUID: "block-1", MessageIDs: []int64{101}},
		{BlockUUID: "block-2", Suppressed: true, MessageIDs: []int64{202}},
	}}
	if !receiptHasPublishedBlock(receipt, "BLOCK-1") {
		t.Fatal("опубликованный блок не распознан")
	}
	if receiptHasPublishedBlock(receipt, "block-2") {
		t.Fatal("подавленный блок ошибочно признан опубликованным")
	}
	if receiptHasPublishedBlock(receipt, "block-3") {
		t.Fatal("несуществующий блок ошибочно признан опубликованным")
	}
}
