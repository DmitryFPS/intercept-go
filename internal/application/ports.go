package application

import (
	"context"
	"errors"
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
	Record        domain.SourceRecord
	Position      domain.Position
	Mode          domain.SourceMode
	Content       string
	Blocks        []domain.ContentBlock
	DeletedBlocks []domain.ContentBlock
	Archived      bool
	// ArchiveMetadataAmbiguous is set when the active EAVC schema has routing
	// columns that the archive table omits.
	ArchiveMetadataAmbiguous bool
	UpdatedBy                string
	ObservedAt               time.Time
	// ContractError изолирует одну несовместимую/повреждённую запись EAVC.
	// Клиент журналирует её и продолжает следующие записи; после исправления
	// оператором новая updated_at снова попадёт в чтение.
	ContractError string
	// logicalTimeBlocks отмечает внутренний снимок после безопасного разбиения
	// физических блоков EAVC на отдельные вхождения времени.
	logicalTimeBlocks bool
}

// ClientQueue гарантирует сохранение события до обращения к сети.
type ClientQueue interface {
	// Enqueue возвращает true только когда новая версия события действительно
	// сохранена. Повторное чтение одной и той же версии из EAVC не считается
	// новой постановкой в очередь.
	Enqueue(ctx context.Context, event domain.Event) (bool, error)
	// SeedBlockBaselinesFromLegacy атомарно переносит уже обработанную цельную
	// версию записи прежнего Клиента в поблочные идентичности. Это не создаёт
	// сообщений и не позволяет обновлению повторно выдать старые блоки.
	SeedBlockBaselinesFromLegacy(ctx context.Context, sourceUUID, sourceVersion string, events []domain.Event) (bool, error)
	// SeedArchiveBaselinesFromSeenBlocks подавляет только доказанный повтор,
	// который возникает при переносе закрытой смены EAVC в архив. EAVC удаляет
	// UUID блоков при архивации, поэтому архивные legacy-UUID можно признать
	// эквивалентными лишь когда ранее были замечены все блоки записи и её
	// последняя версия. Неполная история возвращает false и не теряет данные.
	SeedArchiveBaselinesFromSeenBlocks(ctx context.Context, sourceUUID, sourceVersion string, events []domain.Event) (bool, error)
	Pending(ctx context.Context, limit int, now time.Time) ([]QueuedEvent, error)
	MarkUploaded(ctx context.Context, eventID string, telegramMessageID int64) error
	MarkAcknowledged(ctx context.Context, eventID string, at time.Time) error
	MarkExpired(ctx context.Context, eventID string, at time.Time, reason string) error
	// MarkServerDisposition атомарно завершает событие и сохраняет ID пакета,
	// подтверждённого Центром. Для ACK/3 transportMessageID равен нулю.
	MarkServerDisposition(ctx context.Context, eventID string, at time.Time, status string, transportMessageID int64) (newlyTerminal bool, err error)
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
	EventID            string
	AcceptedAt         time.Time
	Status             string
	TransportMessageID int64
}

// ServerStore распознаёт ранее принятые события и атомарно сохраняет новые.
type ServerStore interface {
	KnownEvent(ctx context.Context, eventID, clientID, contentDigest string) (known bool, err error)
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

// ErrTelegramTopicMissing допускает резервную доставку только после
// окончательного ответа Telegram об отсутствующей теме, не при сбое сети.
var ErrTelegramTopicMissing = errors.New("тема Telegram удалена")

// DeliveryReceipt — подтверждённые внешние ID и состояние публикации.
// Telegram может разбить длинный перехват на несколько сообщений: часть ID уже
// подтверждена, а исход следующей части ещё неизвестен. Для Word и других
// направлений квитанция может быть пустой.
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
	BlockUUID              string  `json:"block_uuid"`
	MessageIDs             []int64 `json:"message_ids,omitempty"`
	DeletedMessageIDs      []int64 `json:"deleted_message_ids,omitempty"`
	ExpectedMessageCount   int     `json:"expected_message_count,omitempty"`
	Complete               bool    `json:"complete,omitempty"`
	Suppressed             bool    `json:"suppressed,omitempty"`
	SuppressedReason       string  `json:"suppressed_reason,omitempty"`
	UnknownOutcome         bool    `json:"unknown_outcome,omitempty"`
	ManualRepublishPending bool    `json:"manual_republish_pending,omitempty"`
	UnconfirmedPart        int     `json:"unconfirmed_part,omitempty"`
	SendAttemptToken       string  `json:"send_attempt_token,omitempty"`
	ObservedMessageID      int64   `json:"observed_message_id,omitempty"`
	ObservedThreadID       int64   `json:"observed_thread_id,omitempty"`
	ObservedChatID         int64   `json:"observed_chat_id,omitempty"`
}

// DeliveryReceiptProvider сообщает подтверждённые внешние ID после успешной
// операции и частичное состояние после ошибки (включая неопределённый исход
// одной из частей). Ошибка чтения результата не должна превращать ранее
// выполненную доставку в ошибочно подтверждённую или создавать дубль.
type DeliveryReceiptProvider interface {
	DeliveryReceipt(event domain.Event, delivery domain.PlannedDelivery) (DeliveryReceipt, error)
}

// DeliveryReceiptQueue расширяет очередь без нарушения старых адаптеров.
type DeliveryReceiptQueue interface {
	MarkDeliveredWithReceipt(ctx context.Context, eventID, destinationID string, at time.Time, receipt DeliveryReceipt) error
}

// AliasFallbackQueue атомарно фиксирует фактическую доставку в General и
// меняет маршрут барьера ревизий, чтобы следующие редакции не создали дубль.
type AliasFallbackQueue interface {
	MarkDeliveredOnAliasFallback(ctx context.Context, eventID, destinationID string, at time.Time, receipt DeliveryReceipt, fallback domain.PlannedDelivery) error
}

// PartialDeliveryReceiptQueue сохраняет подтверждённые части и сведения о
// неопределённом исходе отправки, не переводя задание в состояние «доставлено».
// Это необязательное расширение: старые адаптеры очереди продолжают работать.
type PartialDeliveryReceiptQueue interface {
	SavePartialDeliveryReceipt(ctx context.Context, eventID, destinationID string, receipt DeliveryReceipt) error
}
