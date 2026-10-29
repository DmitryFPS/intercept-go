package application

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"intercept/internal/domain"
	"intercept/internal/protocol"
)

// Сервер принимает файл только от зарегистрированного бота и не доверяет одному имени клиента внутри файла.
func TestServerServiceChecksTelegramIdentityBeforeAccept(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x31}, 32)
	event := serverServiceEvent()
	envelope, err := protocol.Seal(event, "key-1", key, time.Now(), bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	payload, _ := json.Marshal(envelope)
	store := &memoryServerStore{}
	service := ServerService{
		Clients: memoryRegistry{client: RegisteredClient{
			ID: "textile", Number: "001", Name: "Текстиль", SenderBotID: 11, SenderBotUsername: "textile_bot",
			ChatID: -1001, ThreadID: 42, KeyID: "key-1", Key: key, Active: true,
		}},
		Rules: memoryRules{rules: serverServiceRules()}, Store: store,
	}
	packet := IncomingPacket{
		SenderBotID: 11, SenderBotUsername: "textile_bot", ChatID: -1001, ThreadID: 42, Payload: payload,
	}
	if err := service.AuthenticatePacket(context.Background(), packet); err != nil {
		t.Fatalf("подлинный пакет не прошёл раннюю проверку: %v", err)
	}
	if store.accepted != 0 {
		t.Fatal("ранняя проверка не должна сохранять пакет как перехват")
	}
	result, err := service.Receive(context.Background(), packet)
	if err != nil || !result.Accepted || store.accepted != 1 {
		t.Fatalf("receive: %#v %v", result, err)
	}
	if result.ContentKind != "text" || result.ContentChars != len([]rune(event.Content)) ||
		result.ContentBlocks != len(event.Blocks) || result.SourceMode != string(event.Mode) {
		t.Fatalf("журналу возвращена неверная характеристика содержимого: %#v", result)
	}
	store.knownErr = errors.New("database busy")
	if _, err := service.Receive(context.Background(), packet); err == nil || IsPermanentPacketError(err) {
		t.Fatalf("временный сбой чтения БД ошибочно классифицирован как постоянный: %v", err)
	}
	store.knownErr = nil
	packet.Payload = append(append([]byte(nil), payload...), []byte("{}")...)
	if _, err := service.Receive(context.Background(), packet); err == nil {
		t.Fatal("дополнительный JSON после конверта должен быть отклонён")
	}
	packet.Payload = payload
	packet.SenderBotID = 12
	if _, err := service.Receive(context.Background(), packet); err == nil {
		t.Fatal("чужой Telegram-бот должен быть отклонён")
	}
	packet.SenderBotID = 11
	packet.ThreadID = 43
	if _, err := service.Receive(context.Background(), packet); err == nil {
		t.Fatal("чужая тема должна быть отклонена")
	}
	packet.ThreadID = 42
	var changed protocol.Envelope
	_ = json.Unmarshal(payload, &changed)
	changed.KeyID = "чужой-ключ"
	packet.Payload, _ = json.Marshal(changed)
	if _, err := service.Receive(context.Background(), packet); err == nil {
		t.Fatal("незарегистрированный ключ должен быть отклонён")
	}
	_ = json.Unmarshal(payload, &changed)
	ciphertext, _ := base64.RawStdEncoding.DecodeString(changed.Ciphertext)
	ciphertext[len(ciphertext)-1] ^= 0xff
	changed.Ciphertext = base64.RawStdEncoding.EncodeToString(ciphertext)
	packet.Payload, _ = json.Marshal(changed)
	if err := service.AuthenticatePacket(context.Background(), packet); !IsPermanentPacketError(err) {
		t.Fatalf("повреждённый шифротекст должен быть окончательно отвергнут до записи: %v", err)
	}
	if _, err := service.Receive(context.Background(), packet); err == nil {
		t.Fatal("повреждённый шифротекст должен быть отклонён")
	}
	if store.accepted != 1 {
		t.Fatal("отклонённый пакет не должен попадать в хранилище")
	}
}

func TestServerServiceSelectsTraceAckOnlyForOptedInEncryptedEvent(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x31}, 32)
	for _, optedIn := range []bool{false, true} {
		event := serverServiceEvent()
		originalDigest := event.ContentDigest
		event.AckTraceV4 = optedIn
		if event.CanonicalContentDigest() != originalDigest {
			t.Fatal("транспортная возможность изменила отпечаток данных EAVC")
		}
		envelope, err := protocol.Seal(event, "key-1", key, time.Now(), bytes.NewReader(make([]byte, 12)))
		if err != nil {
			t.Fatal(err)
		}
		if envelope.Schema != protocol.SchemaV2 {
			t.Fatalf("включение ACK/4 сломало базовую схему пакета: %s", envelope.Schema)
		}
		payload, _ := json.Marshal(envelope)
		service := ServerService{
			Clients: memoryRegistry{client: RegisteredClient{
				ID: "textile", Number: "001", Name: "Текстиль", SenderBotID: 11, SenderBotUsername: "textile_bot",
				ChatID: -1001, ThreadID: 42, KeyID: "key-1", Key: key, Active: true,
			}},
			Rules: memoryRules{rules: serverServiceRules()}, Store: &memoryServerStore{},
		}
		packet := IncomingPacket{SenderBotID: 11, SenderBotUsername: "textile_bot", ChatID: -1001, ThreadID: 42, Payload: payload}
		result, err := service.Receive(context.Background(), packet)
		if err != nil || !result.Accepted || result.AckTraceV4 != optedIn {
			t.Fatalf("сервер неверно выбрал формат ACK (optedIn=%v): result=%+v err=%v", optedIn, result, err)
		}
	}
}

func TestServerServiceKeepsAuthenticatedMalformedPacketForQuarantine(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x62}, 32)
	nonce := make([]byte, 12)
	envelope := protocol.Envelope{
		Schema: protocol.SchemaV2, ClientID: "textile", EventID: "event-1", KeyID: "key-1",
		Nonce: base64.RawStdEncoding.EncodeToString(nonce),
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	aad := strings.Join([]string{envelope.Schema, envelope.ClientID, envelope.EventID, envelope.KeyID}, "\n")
	envelope.Ciphertext = base64.RawStdEncoding.EncodeToString(gcm.Seal(nil, nonce, []byte("not-json"), []byte(aad)))
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	store := &memoryServerStore{}
	service := ServerService{
		Clients: memoryRegistry{client: RegisteredClient{
			ID: "textile", SenderBotID: 11, SenderBotUsername: "textile_bot",
			ChatID: -1001, ThreadID: 42, KeyID: "key-1", Key: key, Active: true,
		}},
		Rules: memoryRules{rules: serverServiceRules()}, Store: store,
	}
	packet := IncomingPacket{
		SenderBotID: 11, SenderBotUsername: "textile_bot", ChatID: -1001, ThreadID: 42, Payload: payload,
	}
	if err := service.AuthenticatePacket(context.Background(), packet); err != nil {
		t.Fatalf("подлинный файл должен дойти до штатного карантина: %v", err)
	}
	if _, err := service.Receive(context.Background(), packet); !IsPermanentPacketError(err) {
		t.Fatalf("повреждённое событие должно попасть в карантин: %v", err)
	}
	if store.accepted != 0 {
		t.Fatal("повреждённое событие попало в рабочие перехваты")
	}
}

func TestEventContentKindDoesNotInventAttachmentType(t *testing.T) {
	t.Parallel()
	if got := eventContentKind(domain.Event{Content: " \r\n\t"}); got != "empty_text" {
		t.Fatalf("пустой текст классифицирован как %q", got)
	}
	if got := eventContentKind(domain.Event{Content: "Текст перехвата"}); got != "text" {
		t.Fatalf("текст классифицирован как %q", got)
	}
	if got := eventContentKind(domain.Event{SourceBlockDeleted: true}); got != "deleted_block" {
		t.Fatalf("удаление блока классифицировано как %q", got)
	}
}

func TestServerServiceDoesNotExpireBlockDeletionCleanup(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 27, 20, 0, 0, 0, time.UTC)
	key := bytes.Repeat([]byte{0x61}, 32)
	event := serverServiceEvent()
	event.Content = ""
	event.SourceBlockUUID = "block-deleted"
	event.SourceBlockDeleted = true
	event.CreatedAt = now
	event.SourceAt = now
	event.Blocks = []domain.ContentBlock{{UUID: "block-deleted", DeletedAt: now.Format(time.RFC3339Nano)}}
	event.ExpiresAt = time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)
	event.ContentDigest = event.CanonicalContentDigest()
	envelope, err := protocol.Seal(event, "key-1", key, now, bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(envelope)
	store := &memoryServerStore{}
	service := ServerService{
		Clients: memoryRegistry{client: RegisteredClient{
			ID: "textile", SenderBotID: 11, SenderBotUsername: "textile_bot",
			ChatID: -1001, ThreadID: 42, KeyID: "key-1", Key: key, Active: true,
		}},
		Rules: memoryRules{rules: serverServiceRules()}, Store: store,
		Now: func() time.Time { return now }, DeliveryWindow: 10 * time.Minute,
	}
	result, err := service.Receive(context.Background(), IncomingPacket{
		SenderBotID: 11, SenderBotUsername: "textile_bot", ChatID: -1001,
		ThreadID: 42, Payload: payload,
	})
	if err != nil || !result.Accepted || result.ContentKind != "deleted_block" {
		t.Fatalf("удаление блока не принято: result=%#v err=%v", result, err)
	}
	if store.event.ExpiresAt.Year() != 9999 {
		t.Fatalf("очистка ошибочно получила короткий срок: %s", store.event.ExpiresAt)
	}
}

// Своевременно принятый пакет получает отдельные десять минут на внешние
// Telegram- и Word-доставки, даже если клиент прислал его у границы 30 минут.
func TestServerServiceStartsDeliveryWindowAfterAcceptance(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	key := bytes.Repeat([]byte{0x42}, 32)
	event := serverServiceEvent()
	event.ExpiresAt = now.Add(time.Second)
	envelope, err := protocol.Seal(event, "key-1", key, now, bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(envelope)
	store := &memoryServerStore{}
	service := ServerService{
		Clients: memoryRegistry{client: RegisteredClient{
			ID: "textile", SenderBotID: 11, SenderBotUsername: "textile_bot",
			ChatID: -1001, ThreadID: 42, KeyID: "key-1", Key: key, Active: true,
		}},
		Rules: memoryRules{rules: serverServiceRules()}, Store: store,
		Now: func() time.Time { return now }, DeliveryWindow: 10 * time.Minute,
	}
	_, err = service.Receive(context.Background(), IncomingPacket{
		SenderBotID: 11, SenderBotUsername: "textile_bot", ChatID: -1001,
		ThreadID: 42, Payload: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := now.Add(10 * time.Minute); !store.event.ExpiresAt.Equal(want) {
		t.Fatalf("срок доставки %s, ожидался %s", store.event.ExpiresAt, want)
	}
}

// Граница серверного окна считается от приёма пакета, а не от сохранения в
// EAVC. Управляемое время позволяет проверить оба края без ожидания двух часов.
func TestServerDeliveryWindowExactTwoHourBoundary(t *testing.T) {
	t.Parallel()
	acceptedAt := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	key := bytes.Repeat([]byte{0x58}, 32)
	event := serverServiceEvent()
	event.ExpiresAt = acceptedAt.Add(time.Second)
	envelope, err := protocol.Seal(event, "key-1", key, acceptedAt, bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	store := &memoryServerStore{}
	reception := ServerService{
		Clients: memoryRegistry{client: RegisteredClient{
			ID: "textile", SenderBotID: 11, SenderBotUsername: "textile_bot",
			ChatID: -1001, ThreadID: 42, KeyID: "key-1", Key: key, Active: true,
		}},
		Rules: memoryRules{rules: serverServiceRules()}, Store: store,
		Now: func() time.Time { return acceptedAt }, DeliveryWindow: 120 * time.Minute,
	}
	result, err := reception.Receive(context.Background(), IncomingPacket{
		SenderBotID: 11, SenderBotUsername: "textile_bot", ChatID: -1001,
		ThreadID: 42, Payload: payload,
	})
	if err != nil || !result.Accepted {
		t.Fatalf("своевременный пакет не принят: result=%#v err=%v", result, err)
	}
	if want := acceptedAt.Add(120 * time.Minute); !store.event.ExpiresAt.Equal(want) {
		t.Fatalf("срок доставки %s, ожидался %s", store.event.ExpiresAt, want)
	}

	tests := []struct {
		name      string
		elapsed   time.Duration
		delivered bool
	}{
		{name: "119m59s", elapsed: 119*time.Minute + 59*time.Second, delivered: true},
		{name: "120m00s", elapsed: 120 * time.Minute, delivered: true},
		{name: "120m01s", elapsed: 120*time.Minute + time.Second, delivered: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queue := &memoryDeliveryQueue{jobs: []DeliveryJob{{
				Event:    store.event,
				Delivery: domain.PlannedDelivery{Destination: domain.Destination{ID: "telegram"}},
			}}}
			delivery := DeliveryService{
				Queue:    queue,
				Gateways: map[string]DeliveryGateway{"telegram": memoryGateway{}},
				Now:      func() time.Time { return acceptedAt.Add(test.elapsed) },
			}
			count, err := delivery.RunOnce(context.Background(), 1)
			if err != nil {
				t.Fatal(err)
			}
			if test.delivered {
				if count != 1 || len(queue.delivered) != 1 || len(queue.retried) != 0 {
					t.Fatalf("в пределах окна доставка не состоялась: count=%d delivered=%v expired=%v",
						count, queue.delivered, queue.retried)
				}
			} else if count != 0 || len(queue.delivered) != 0 ||
				len(queue.retried) != 1 || queue.retried[0] != store.event.ID+":telegram:expired" {
				t.Fatalf("после окна доставка не завершилась как просроченная: count=%d delivered=%v expired=%v",
					count, queue.delivered, queue.retried)
			}
		})
	}
}

// Задержка уже скачанного пакета в локальной очереди не превращает своевременный
// перехват в просроченный: граница свежести фиксируется при скачивании.
func TestServerServiceUsesDurableIngressTimeForFreshness(t *testing.T) {
	t.Parallel()
	downloadedAt := time.Date(2026, 10, 27, 10, 29, 50, 0, time.UTC)
	processedAt := downloadedAt.Add(20 * time.Minute)
	key := bytes.Repeat([]byte{0x47}, 32)
	event := serverServiceEvent()
	event.ExpiresAt = downloadedAt.Add(10 * time.Second)
	envelope, err := protocol.Seal(event, "key-1", key, downloadedAt, bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(envelope)
	store := &memoryServerStore{}
	service := ServerService{
		Clients: memoryRegistry{client: RegisteredClient{
			ID: "textile", SenderBotID: 11, SenderBotUsername: "textile_bot",
			ChatID: -1001, ThreadID: 42, KeyID: "key-1", Key: key, Active: true,
		}},
		Rules: memoryRules{rules: serverServiceRules()}, Store: store,
		Now: func() time.Time { return processedAt }, DeliveryWindow: 10 * time.Minute,
	}
	result, err := service.Receive(context.Background(), IncomingPacket{
		SenderBotID: 11, SenderBotUsername: "textile_bot", ChatID: -1001,
		ThreadID: 42, Payload: payload, ReceivedAt: downloadedAt,
	})
	if err != nil || !result.Accepted || result.Status != "accepted" {
		t.Fatalf("своевременно скачанный пакет потерян в локальной очереди: %#v, %v", result, err)
	}
	if !store.event.ExpiresAt.Equal(processedAt.Add(10 * time.Minute)) {
		t.Fatalf("внешнее окно не началось после обработки: %s", store.event.ExpiresAt)
	}
}

// Если первое подтверждение потерялось, сервер повторно подтверждает уже
// сохранённое событие, даже когда его исходные 30 минут успели закончиться.
func TestServerServiceAcknowledgesKnownEventAfterTransportDeadline(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	key := bytes.Repeat([]byte{0x53}, 32)
	event := serverServiceEvent()
	event.ExpiresAt = now.Add(time.Minute)
	envelope, err := protocol.Seal(event, "key-1", key, now, bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(envelope)
	store := &memoryServerStore{}
	service := ServerService{
		Clients: memoryRegistry{client: RegisteredClient{
			ID: "textile", SenderBotID: 11, SenderBotUsername: "textile_bot",
			ChatID: -1001, ThreadID: 42, KeyID: "key-1", Key: key, Active: true,
		}},
		Rules: memoryRules{rules: serverServiceRules()}, Store: store,
		Now: func() time.Time { return now },
	}
	packet := IncomingPacket{
		SenderBotID: 11, SenderBotUsername: "textile_bot", ChatID: -1001,
		ThreadID: 42, Payload: payload,
	}
	first, err := service.Receive(context.Background(), packet)
	if err != nil || !first.Accepted {
		t.Fatalf("первый приём: %#v %v", first, err)
	}
	now = now.Add(31 * time.Minute)
	repeated, err := service.Receive(context.Background(), packet)
	if err != nil || repeated.Status != "accepted" || repeated.Accepted {
		t.Fatalf("повтор после срока: %#v %v", repeated, err)
	}
	if store.accepted != 1 {
		t.Fatalf("повтор создал вторую запись: %d", store.accepted)
	}
}

func TestServerServiceKeepsPacketOnTransientRegistryFailure(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x61}, 32)
	event := serverServiceEvent()
	envelope, err := protocol.Seal(event, "key-1", key, time.Now(), bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(envelope)
	service := ServerService{
		Clients: memoryRegistry{err: errors.New("database is temporarily locked")},
		Rules:   memoryRules{rules: serverServiceRules()}, Store: &memoryServerStore{},
	}
	_, err = service.Receive(context.Background(), IncomingPacket{Payload: payload})
	if err == nil || IsPermanentPacketError(err) {
		t.Fatalf("временная ошибка реестра ошибочно разрешает удалить пакет: %v", err)
	}
	service.Clients = memoryRegistry{}
	_, err = service.Receive(context.Background(), IncomingPacket{Payload: payload})
	if err == nil || !IsPermanentPacketError(err) {
		t.Fatalf("неизвестная точка не классифицирована как окончательный отказ: %v", err)
	}
}

func TestServerServiceRejectsDigestAndFutureRevisionPoisoning(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	key := bytes.Repeat([]byte{0x71}, 32)
	receive := func(event domain.Event) error {
		envelope, err := protocol.Seal(event, "key-1", key, now, bytes.NewReader(make([]byte, 12)))
		if err != nil {
			return err
		}
		payload, _ := json.Marshal(envelope)
		service := ServerService{
			Clients: memoryRegistry{client: RegisteredClient{
				ID: "textile", SenderBotID: 11, SenderBotUsername: "textile_bot",
				ChatID: -1001, ThreadID: 42, KeyID: "key-1", Key: key, Active: true,
			}},
			Rules: memoryRules{rules: serverServiceRules()}, Store: &memoryServerStore{},
			Now: func() time.Time { return now },
		}
		_, err = service.Receive(context.Background(), IncomingPacket{
			SenderBotID: 11, SenderBotUsername: "textile_bot", ChatID: -1001,
			ThreadID: 42, Payload: payload,
		})
		return err
	}
	event := serverServiceEvent()
	event.CreatedAt = now
	event.SourceAt = now
	event.Source.UpdatedAt = "2026-10-28 12:00:00"
	event.ContentDigestVersion = ""
	event.ContentDigest = event.CanonicalContentDigest()
	if err := receive(event); err == nil {
		t.Fatal("пакет старого контракта без версии digest принят")
	}
	event.ContentDigestVersion = domain.ContentDigestVersionCurrent
	event.ContentDigest = event.CanonicalContentDigest()
	event.ContentDigest = "подменённый-отпечаток"
	if err := receive(event); err == nil {
		t.Fatal("подмена digest не отклонена до отправки")
	}
	event.ContentDigest = event.CanonicalContentDigest()
	event.Source.UpdatedAt = "9999-10-28 12:00:00"
	if err := receive(event); err == nil {
		t.Fatal("будущая ревизия не отклонена")
	}

	current := serverServiceEvent()
	current.CreatedAt, current.SourceAt = now, now
	current.Source.UpdatedAt = "2026-10-28 12:00:00"
	current.Content = "12.00\n-Доклад. (61021)"
	current.SourceBlockUUID = "block-1"
	current.HasActiveBlocks = true
	current.Blocks = []domain.ContentBlock{{
		UUID: "block-1", Text: current.Content, UpdatedAt: current.Source.UpdatedAt,
		Callsigns: []domain.Callsign{{Code: "61021", Label: "Рубин"}},
	}}
	current.ContentDigestVersion = domain.ContentDigestVersionCurrent
	current.ContentDigest = current.CanonicalContentDigest()
	if err := receive(current); err != nil {
		t.Fatalf("корректный пакет v2 отклонён: %v", err)
	}
	tamperedCatalog := current
	tamperedCatalog.Position.CatalogUUID = "подменённый-каталог"
	if err := receive(tamperedCatalog); err == nil {
		t.Fatal("подмена catalog_uuid не отклонена до отправки")
	}
	tamperedCallsign := current
	tamperedCallsign.Blocks = append([]domain.ContentBlock(nil), current.Blocks...)
	tamperedCallsign.Blocks[0].Callsigns = append([]domain.Callsign(nil), current.Blocks[0].Callsigns...)
	tamperedCallsign.Blocks[0].Callsigns[0].Label = "Подменённый"
	if err := receive(tamperedCallsign); err == nil {
		t.Fatal("подмена позывного не отклонена до отправки")
	}
	mismatchedText := current
	mismatchedText.Content = "другой текст"
	mismatchedText.ContentDigest = mismatchedText.CanonicalContentDigest()
	if err := receive(mismatchedText); err == nil {
		t.Fatal("расхождение Content и блока разрешено упаковать")
	}
	unknownVersion := current
	unknownVersion.ContentDigestVersion = "v999"
	if err := receive(unknownVersion); err == nil {
		t.Fatal("неизвестную версию digest разрешено упаковать")
	}
}

// Ошибка одной конечной группы не должна мешать отправке того же перехвата в остальные назначенные места.
func TestDeliveryServiceKeepsDestinationFailuresIndependent(t *testing.T) {
	t.Parallel()

	queue := &memoryDeliveryQueue{jobs: []DeliveryJob{
		{Event: serverServiceEvent(), Delivery: domain.PlannedDelivery{Destination: domain.Destination{ID: "ok"}}},
		{Event: serverServiceEvent(), Delivery: domain.PlannedDelivery{Destination: domain.Destination{ID: "fail"}}},
	}}
	service := DeliveryService{
		Queue: queue,
		Gateways: map[string]DeliveryGateway{
			"ok": memoryGateway{}, "fail": memoryGateway{err: errors.New("нет сети")},
		},
		Now: time.Now, MaxConcurrent: 1,
	}
	count, err := service.RunOnce(context.Background(), 10)
	if err != nil || count != 1 {
		t.Fatalf("run: %d %v", count, err)
	}
	if len(queue.delivered) != 1 || len(queue.retried) != 1 {
		t.Fatalf("состояния направлений смешались: %#v %#v", queue.delivered, queue.retried)
	}
}

func TestDeliveryServicePersistsExternalReceipt(t *testing.T) {
	t.Parallel()
	event := serverServiceEvent()
	delivery := domain.PlannedDelivery{Destination: domain.Destination{ID: "telegram", Kind: domain.DestinationTelegram}}
	queue := &memoryDeliveryQueue{jobs: []DeliveryJob{{Event: event, Delivery: delivery}}}
	service := DeliveryService{
		Queue: queue, Gateways: map[string]DeliveryGateway{"telegram": receiptGateway{}}, Now: time.Now,
	}
	delivered, err := service.RunOnce(context.Background(), 10)
	if err != nil || delivered != 1 || len(queue.receipts) != 1 ||
		len(queue.receipts[0].MessageIDs) != 1 || queue.receipts[0].MessageIDs[0] != 701 {
		t.Fatalf("результат внешней доставки не сохранён: delivered=%d receipts=%#v err=%v", delivered, queue.receipts, err)
	}
}

func TestDeliveryServiceRetriesWhenPublishedReceiptIsTemporarilyUnavailable(t *testing.T) {
	t.Parallel()
	event := serverServiceEvent()
	delivery := domain.PlannedDelivery{Destination: domain.Destination{ID: "telegram", Kind: domain.DestinationTelegram}}
	queue := &memoryDeliveryQueue{jobs: []DeliveryJob{{Event: event, Delivery: delivery}}}
	gateway := &flakyReceiptGateway{}
	service := DeliveryService{
		Queue: queue, Gateways: map[string]DeliveryGateway{"telegram": gateway}, Now: time.Now,
	}
	delivered, err := service.RunOnce(context.Background(), 10)
	if err != nil || delivered != 0 || len(queue.retried) != 1 ||
		len(queue.delivered) != 0 || len(queue.receipts) != 0 {
		t.Fatalf("задание завершено без внешнего ID: delivered=%d retried=%v receipts=%#v err=%v",
			delivered, queue.retried, queue.receipts, err)
	}
	delivered, err = service.RunOnce(context.Background(), 10)
	if err != nil || delivered != 1 || len(queue.delivered) != 1 || len(queue.receipts) != 1 ||
		!reflect.DeepEqual(queue.receipts[0].MessageIDs, []int64{701}) {
		t.Fatalf("повтор не восстановил квитанцию: delivered=%d receipts=%#v err=%v",
			delivered, queue.receipts, err)
	}
}

// Неисправимое редактирование завершается один раз и не превращается в
// бесконечный повтор, удерживающий следующие сообщения очереди.
func TestDeliveryServiceFinalizesPermanentFailure(t *testing.T) {
	t.Parallel()

	queue := &memoryDeliveryQueue{jobs: []DeliveryJob{
		{Event: serverServiceEvent(), Delivery: domain.PlannedDelivery{Destination: domain.Destination{ID: "telegram"}}},
	}}
	service := DeliveryService{
		Queue: queue,
		Gateways: map[string]DeliveryGateway{
			"telegram": memoryGateway{err: NewPermanentDeliveryError(errors.New("сообщение удалено"))},
		},
		Now: time.Now, MaxConcurrent: 1,
	}
	count, err := service.RunOnce(context.Background(), 10)
	if err != nil || count != 0 {
		t.Fatalf("run: %d %v", count, err)
	}
	if len(queue.failed) != 1 || len(queue.retried) != 0 {
		t.Fatalf("неисправимая доставка не завершена: failed=%#v retried=%#v", queue.failed, queue.retried)
	}
}

// Зависшая отправка в один топик не должна останавливать сообщения для другого топика.
func TestDeliveryServiceDoesNotLetSlowStreamBlockAnother(t *testing.T) {
	t.Parallel()
	queue := &memoryDeliveryQueue{jobs: []DeliveryJob{
		{Event: serverServiceEvent(), Delivery: domain.PlannedDelivery{Destination: domain.Destination{ID: "slow"}}},
		{Event: serverServiceEvent(), Delivery: domain.PlannedDelivery{Destination: domain.Destination{ID: "fast"}}},
	}}
	release := make(chan struct{})
	fastDone := make(chan struct{})
	service := DeliveryService{
		Queue: queue, MaxConcurrent: 2, AttemptTimeout: time.Second,
		Gateways: map[string]DeliveryGateway{
			"slow": channelGateway{release: release},
			"fast": channelGateway{done: fastDone},
		},
		Now: time.Now,
	}
	finished := make(chan error, 1)
	go func() {
		_, err := service.RunOnce(context.Background(), 10)
		finished <- err
	}()
	select {
	case <-fastDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("быстрое направление заблокировано медленным")
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

type memoryRegistry struct {
	client RegisteredClient
	err    error
}

func (registry memoryRegistry) Client(_ context.Context, id string) (RegisteredClient, error) {
	if registry.err != nil {
		return RegisteredClient{}, registry.err
	}
	if id != registry.client.ID {
		return RegisteredClient{}, ErrClientNotFound
	}
	return registry.client, nil
}

func (registry memoryRegistry) SaveClient(_ context.Context, _ RegisteredClient) error { return nil }

type memoryRules struct{ rules domain.RuleSet }

func (rules memoryRules) ActiveRules(_ context.Context) (domain.RuleSet, error) {
	return rules.rules, nil
}
func (rules memoryRules) PublishRules(_ context.Context, _ domain.RuleSet, _ string) error {
	return nil
}

type memoryServerStore struct {
	accepted int
	event    domain.Event
	knownErr error
}

func (store *memoryServerStore) KnownEvent(_ context.Context, eventID, clientID, contentDigest string) (bool, error) {
	if store.knownErr != nil {
		return false, store.knownErr
	}
	if store.accepted == 0 || store.event.ID != eventID {
		return false, nil
	}
	if store.event.ClientID != clientID || store.event.ContentDigest != contentDigest {
		return false, ErrEventIDConflict
	}
	return true, nil
}

func (store *memoryServerStore) Accept(_ context.Context, event domain.Event, _ domain.DeliveryPlan) (bool, error) {
	store.accepted++
	store.event = event
	return true, nil
}

type memoryGateway struct{ err error }

func (gateway memoryGateway) Deliver(_ context.Context, _ domain.Event, _ domain.PlannedDelivery) error {
	return gateway.err
}

type receiptGateway struct{}

func (receiptGateway) Deliver(_ context.Context, _ domain.Event, _ domain.PlannedDelivery) error {
	return nil
}

func (receiptGateway) DeliveryReceipt(_ domain.Event, _ domain.PlannedDelivery) (DeliveryReceipt, error) {
	return DeliveryReceipt{Kind: "telegram", ChatID: -1001, ThreadID: 17, MessageIDs: []int64{701}}, nil
}

type flakyReceiptGateway struct{ reads atomic.Int32 }

func (*flakyReceiptGateway) Deliver(_ context.Context, _ domain.Event, _ domain.PlannedDelivery) error {
	return nil
}

func (gateway *flakyReceiptGateway) DeliveryReceipt(_ domain.Event, _ domain.PlannedDelivery) (DeliveryReceipt, error) {
	if gateway.reads.Add(1) == 1 {
		return DeliveryReceipt{}, errors.New("временная ошибка чтения состояния")
	}
	return DeliveryReceipt{Kind: "telegram", ChatID: -1001, ThreadID: 17, MessageIDs: []int64{701}}, nil
}

type channelGateway struct {
	release <-chan struct{}
	done    chan<- struct{}
}

func (gateway channelGateway) Deliver(ctx context.Context, _ domain.Event, _ domain.PlannedDelivery) error {
	if gateway.done != nil {
		close(gateway.done)
		return nil
	}
	select {
	case <-gateway.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type memoryDeliveryQueue struct {
	mu        sync.Mutex
	jobs      []DeliveryJob
	delivered []string
	retried   []string
	failed    []string
	receipts  []DeliveryReceipt
}

func (queue *memoryDeliveryQueue) PendingDeliveries(_ context.Context, _ int, _ time.Time) ([]DeliveryJob, error) {
	return queue.jobs, nil
}

func TestLimitParallelWordDestinations(t *testing.T) {
	t.Parallel()
	word := domain.Destination{ID: "word-39", Kind: domain.DestinationWord}
	otherWord := domain.Destination{ID: "word-archive", Kind: domain.DestinationWord}
	telegram := domain.Destination{ID: "telegram", Kind: domain.DestinationTelegram}
	jobs := []DeliveryJob{
		{Delivery: domain.PlannedDelivery{Destination: word}},
		{Delivery: domain.PlannedDelivery{Destination: telegram}},
		{Delivery: domain.PlannedDelivery{Destination: word}},
		{Delivery: domain.PlannedDelivery{Destination: otherWord}},
		{Delivery: domain.PlannedDelivery{Destination: telegram}},
	}

	selected := limitParallelWordDestinations(jobs)
	if len(selected) != 4 {
		t.Fatalf("ожидалось четыре независимые операции, получено %d", len(selected))
	}
	wordCount := 0
	telegramCount := 0
	for _, job := range selected {
		switch job.Delivery.Destination.ID {
		case word.ID:
			wordCount++
		case telegram.ID:
			telegramCount++
		}
	}
	if wordCount != 1 || telegramCount != 2 {
		t.Fatalf("Word=%d, Telegram=%d; Word должен быть последовательным, Telegram — параллельным", wordCount, telegramCount)
	}
}

func (queue *memoryDeliveryQueue) MarkDelivered(_ context.Context, eventID, destinationID string, _ time.Time) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.delivered = append(queue.delivered, eventID+":"+destinationID)
	return nil
}

func (queue *memoryDeliveryQueue) MarkDeliveredWithReceipt(
	_ context.Context, eventID, destinationID string, _ time.Time, receipt DeliveryReceipt,
) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.delivered = append(queue.delivered, eventID+":"+destinationID)
	queue.receipts = append(queue.receipts, receipt)
	return nil
}

func (queue *memoryDeliveryQueue) MarkDeliveryExpired(_ context.Context, eventID, destinationID string, _ time.Time, _ string) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.retried = append(queue.retried, eventID+":"+destinationID+":expired")
	return nil
}

func (queue *memoryDeliveryQueue) MarkDeliveryFailed(_ context.Context, eventID, destinationID string, _ time.Time, _ string) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.failed = append(queue.failed, eventID+":"+destinationID)
	return nil
}

func (queue *memoryDeliveryQueue) MarkDeliveryRetry(_ context.Context, eventID, destinationID string, _ time.Time, _ string) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.retried = append(queue.retried, eventID+":"+destinationID)
	return nil
}

func serverServiceEvent() domain.Event {
	now := time.Date(2026, 10, 19, 11, 0, 0, 0, time.UTC)
	event := domain.Event{
		ID: "event-1", ClientID: "textile", CreatedAt: now, ObservedAt: now,
		Source:   domain.SourceRecord{ID: 1, UUID: "source"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "данные", ContentDigestVersion: domain.ContentDigestVersionCurrent,
	}
	event.ContentDigest = event.CanonicalContentDigest()
	return event
}

func serverServiceRules() domain.RuleSet {
	return domain.RuleSet{
		Version: 1,
		Destinations: map[string]domain.Destination{
			"d": {ID: "d", Name: "Получатель", Kind: domain.DestinationTelegram, Enabled: true},
		},
		Positions: map[string]map[string]domain.RouteDecision{
			"name:" + domain.NormalizeUnitName("позиция"): {"d": {Action: domain.RouteDeliver}},
		},
		Unknown: map[string]domain.RouteDecision{"d": {Action: domain.RouteReview}},
	}
}
