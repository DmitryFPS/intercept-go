package application

import (
	"context"
	"time"

	"intercept/internal/domain"
	"intercept/internal/protocol"
)

// EventSource читает внешнюю систему без изменения её данных.
type EventSource interface {
	Scan(ctx context.Context, after Cursor, limit int) ([]SourceItem, Cursor, error)
}

// Cursor задаёт воспроизводимую позицию чтения источника.
type Cursor struct {
	UpdatedAt string `json:"updated_at"`
	ID        int64  `json:"id"`
}

// SourceItem — снимок записи источника до создания доменного события.
type SourceItem struct {
	Record     domain.SourceRecord
	Position   domain.Position
	Mode       domain.SourceMode
	Content    string
	UpdatedBy  string
	ObservedAt time.Time
}

// ClientQueue гарантирует сохранение события до обращения к сети.
type ClientQueue interface {
	Enqueue(ctx context.Context, event domain.Event) error
	Pending(ctx context.Context, limit int, now time.Time) ([]QueuedEvent, error)
	MarkUploaded(ctx context.Context, eventID string, telegramMessageID int64) error
	MarkAcknowledged(ctx context.Context, eventID string, at time.Time) error
	MarkExpired(ctx context.Context, eventID string, at time.Time, reason string) error
	ExpireDue(ctx context.Context, now time.Time) (int, error)
	MarkRetry(ctx context.Context, eventID string, next time.Time, reason string) error
}

// QueuedEvent содержит состояние попыток, не смешивая его с данными EAVC.
type QueuedEvent struct {
	Event    domain.Event
	Attempts int
}

// ClientState хранит курсор источника, признаки и смещения транспорта.
type ClientState interface {
	Cursor(ctx context.Context, name string) (Cursor, error)
	SaveCursor(ctx context.Context, name string, cursor Cursor) error
	Value(ctx context.Context, name string) (string, bool, error)
	SaveValue(ctx context.Context, name, value string) error
}

// TransportSender загружает зашифрованный конверт и получает подтверждения.
type TransportSender interface {
	Send(ctx context.Context, envelope protocol.Envelope) (messageID int64, err error)
	Acknowledgements(ctx context.Context, offset int64) ([]Acknowledgement, int64, error)
}

// Acknowledgement подтверждает долговечное сохранение события сервером.
type Acknowledgement struct {
	EventID    string
	AcceptedAt time.Time
	Status     string
}

// ServerStore атомарно сохраняет входное событие и его задания доставки.
type ServerStore interface {
	Accept(ctx context.Context, event domain.Event, plan domain.DeliveryPlan) (accepted bool, err error)
}

// RegisteredClient — доверенная запись точки на сервере.
type RegisteredClient struct {
	ID                string
	Number            string
	Name              string
	SenderBotID       int64
	SenderBotUsername string
	ChatID            int64
	ThreadID          int64
	KeyID             string
	Key               []byte
	Active            bool
}

// ClientRegistry управляет точками без изменения исполняемого файла.
type ClientRegistry interface {
	Client(ctx context.Context, id string) (RegisteredClient, error)
	SaveClient(ctx context.Context, client RegisteredClient) error
}

// RuleRepository возвращает только целиком опубликованную версию маршрутов.
type RuleRepository interface {
	ActiveRules(ctx context.Context) (domain.RuleSet, error)
	PublishRules(ctx context.Context, rules domain.RuleSet, author string) error
}

// DeliveryJob связывает исходное событие с одним независимым получателем.
type DeliveryJob struct {
	Event    domain.Event
	Delivery domain.PlannedDelivery
	Attempts int
}

// DeliveryQueue хранит результаты каждого направления отдельно.
type DeliveryQueue interface {
	PendingDeliveries(ctx context.Context, limit int, now time.Time) ([]DeliveryJob, error)
	MarkDelivered(ctx context.Context, eventID, destinationID string, at time.Time) error
	MarkDeliveryExpired(ctx context.Context, eventID, destinationID string, at time.Time, reason string) error
	MarkDeliveryRetry(ctx context.Context, eventID, destinationID string, next time.Time, reason string) error
}

// DeliveryGateway выполняет одно внешнее задание.
type DeliveryGateway interface {
	Deliver(ctx context.Context, event domain.Event, delivery domain.PlannedDelivery) error
}
