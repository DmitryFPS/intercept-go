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

// EventSourceHead фиксирует верхнюю границу существующих данных. Она нужна,
// чтобы первый запуск без догрузки старого не пропустил строки, появившиеся
// одновременно с построением базовой линии.
type EventSourceHead interface {
	Head(ctx context.Context) (Cursor, error)
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
	Blocks     []domain.ContentBlock
	UpdatedBy  string
	ObservedAt time.Time
}

// ClientQueue гарантирует сохранение события до обращения к сети.
type ClientQueue interface {
	// Enqueue возвращает true только когда новая версия события действительно
	// сохранена. Повторное чтение одной и той же версии из EAVC не считается
	// новой постановкой в очередь.
	Enqueue(ctx context.Context, event domain.Event) (bool, error)
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
	State    string
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

// ServerStore распознаёт ранее принятые события и атомарно сохраняет новые.
type ServerStore interface {
	KnownEvent(ctx context.Context, eventID, contentDigest string) (known bool, err error)
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
	MarkDeliveryFailed(ctx context.Context, eventID, destinationID string, at time.Time, reason string) error
	MarkDeliveryRetry(ctx context.Context, eventID, destinationID string, next time.Time, reason string) error
}

// DeliveryGateway выполняет одно внешнее задание.
type DeliveryGateway interface {
	Deliver(ctx context.Context, event domain.Event, delivery domain.PlannedDelivery) error
}

// DeliveryReceipt — подтверждённый внешний результат уже выполненной доставки.
// Telegram может разбить длинный перехват на несколько сообщений, поэтому ID
// хранится списком. Для Word и других направлений результат может быть пустым.
type DeliveryReceipt struct {
	Kind       string                 `json:"kind"`
	ChatID     int64                  `json:"chat_id,omitempty"`
	ThreadID   int64                  `json:"thread_id,omitempty"`
	MessageIDs []int64                `json:"message_ids,omitempty"`
	Blocks     []DeliveryBlockReceipt `json:"blocks,omitempty"`
}

// DeliveryBlockReceipt связывает бизнес-перехват с конечной публикацией.
// Удалённый вручную блок остаётся видимым в аудите, но не попадает в очередь.
type DeliveryBlockReceipt struct {
	BlockUUID         string  `json:"block_uuid"`
	MessageIDs        []int64 `json:"message_ids,omitempty"`
	DeletedMessageIDs []int64 `json:"deleted_message_ids,omitempty"`
	Suppressed        bool    `json:"suppressed,omitempty"`
	SuppressedReason  string  `json:"suppressed_reason,omitempty"`
}

// DeliveryReceiptProvider сообщает фактические ID только после успешной
// внешней операции. Ошибка чтения результата не должна повторять уже
// выполненную доставку и создавать дубль.
type DeliveryReceiptProvider interface {
	DeliveryReceipt(event domain.Event, delivery domain.PlannedDelivery) (DeliveryReceipt, error)
}

// DeliveryReceiptQueue расширяет очередь без нарушения старых адаптеров.
type DeliveryReceiptQueue interface {
	MarkDeliveredWithReceipt(ctx context.Context, eventID, destinationID string, at time.Time, receipt DeliveryReceipt) error
}
