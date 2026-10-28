package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"intercept/internal/application"
)

// Сервер отвергает файл от неизвестного бота ещё до загрузки его содержимого.
func TestServerReceiverRejectsUnknownBotBeforeDownload(t *testing.T) {
	t.Parallel()
	var downloads atomic.Int32
	var observations []application.TransportObservation
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true,
				"result": []any{map[string]any{
					"update_id": 7,
					"message": map[string]any{
						"message_id": 15, "message_thread_id": 9,
						"from":     map[string]any{"id": 999, "is_bot": true, "username": "unknown_bot"},
						"chat":     map[string]any{"id": -1001, "type": "supergroup"},
						"document": map[string]any{"file_id": "malicious", "file_name": "x.icevent", "file_size": 100},
					},
				}},
			})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			downloads.Add(1)
			http.Error(writer, "getFile не должен вызываться", http.StatusInternalServerError)
		default:
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		}
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := ServerReceiver{
		API: client, ChatID: -1001, MaxBytes: 1024,
		Authorize: func(context.Context, application.IncomingPacket) (bool, error) { return false, nil },
		Observe:   func(item application.TransportObservation) { observations = append(observations, item) },
	}
	packets, next, err := receiver.Packets(context.Background(), 0)
	if err != nil || len(packets) != 0 || next != 8 {
		t.Fatalf("неожиданный результат: packets=%#v next=%d err=%v", packets, next, err)
	}
	if downloads.Load() != 0 {
		t.Fatal("сервер запросил файл неизвестного бота")
	}
	if len(observations) != 1 || observations[0].Outcome != "ignored_unauthorized" || observations[0].MessageID != 15 {
		t.Fatalf("отказ неизвестного бота не объяснён в журнале: %#v", observations)
	}
}

// Служебный файл недопустимого размера окончательно отвергается без скачивания
// и передаётся долговечной очереди очистки, чтобы не копиться в Telegram.
func TestServerReceiverDiscardsInvalidSizeTransportFile(t *testing.T) {
	t.Parallel()
	var downloads atomic.Int32
	var discarded atomic.Int64
	var observations []application.TransportObservation
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true,
				"result": []any{map[string]any{
					"update_id": 17,
					"message": map[string]any{
						"message_id": 25, "message_thread_id": 9,
						"from": map[string]any{"id": 101, "is_bot": true, "username": "station_bot"},
						"chat": map[string]any{"id": -1001, "type": "supergroup"},
						"document": map[string]any{
							"file_id": "oversized", "file_name": "oversized.icevent", "file_size": 2048,
						},
					},
				}},
			})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			downloads.Add(1)
			http.Error(writer, "getFile не должен вызываться", http.StatusInternalServerError)
		default:
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		}
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := ServerReceiver{
		API: client, ChatID: -1001, MaxBytes: 1024,
		Authorize: func(context.Context, application.IncomingPacket) (bool, error) { return true, nil },
		Discard:   func(_ context.Context, messageID int64) { discarded.Store(messageID) },
		Observe:   func(item application.TransportObservation) { observations = append(observations, item) },
	}
	packets, next, err := receiver.Packets(context.Background(), 0)
	if err != nil || len(packets) != 0 || next != 18 {
		t.Fatalf("неожиданный результат: packets=%#v next=%d err=%v", packets, next, err)
	}
	if downloads.Load() != 0 || discarded.Load() != 25 {
		t.Fatalf("неверная обработка размера: downloads=%d discarded=%d", downloads.Load(), discarded.Load())
	}
	if len(observations) != 1 || observations[0].Outcome != "ignored_invalid_size" {
		t.Fatalf("невалидный размер не объяснён в журнале: %#v", observations)
	}
}

// Текст, картинка и файл человека в группе обмена классифицируются, но не
// превращаются в рабочий пакет и не требуют скачивания.
func TestServerReceiverAuditsHumanMessagesWithoutCountingThem(t *testing.T) {
	t.Parallel()
	var downloads atomic.Int32
	var observations []application.TransportObservation
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": []any{
				map[string]any{"update_id": 1, "message": map[string]any{
					"message_id": 101, "date": time.Now().Unix(),
					"from": map[string]any{"id": 5, "is_bot": false, "username": "operator"},
					"chat": map[string]any{"id": -1001, "type": "supergroup"}, "text": "ручной текст",
				}},
				map[string]any{"update_id": 2, "message": map[string]any{
					"message_id": 102, "date": time.Now().Unix(),
					"from":     map[string]any{"id": 5, "is_bot": false, "username": "operator"},
					"chat":     map[string]any{"id": -1001, "type": "supergroup"},
					"document": map[string]any{"file_id": "photo", "file_name": "photo.jpg", "file_size": 100},
				}},
				map[string]any{"update_id": 3, "edited_message": map[string]any{
					"message_id": 103, "date": time.Now().Unix(),
					"from": map[string]any{"id": 5, "is_bot": false, "username": "operator"},
					"chat": map[string]any{"id": -1001, "type": "supergroup"}, "text": "отредактированный текст",
				}},
			}})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			downloads.Add(1)
			http.Error(writer, "не должен скачиваться", http.StatusInternalServerError)
		default:
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		}
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := ServerReceiver{
		API: client, ChatID: -1001, MaxBytes: 1024,
		Observe: func(item application.TransportObservation) { observations = append(observations, item) },
	}
	packets, next, err := receiver.Packets(context.Background(), 0)
	if err != nil || len(packets) != 0 || next != 4 || downloads.Load() != 0 {
		t.Fatalf("чужие сообщения затронули транспорт: packets=%#v next=%d downloads=%d err=%v",
			packets, next, downloads.Load(), err)
	}
	if len(observations) != 3 || observations[0].Outcome != "ignored_non_transport" ||
		observations[1].Outcome != "ignored_non_transport" ||
		observations[2].MessageKind != "user_text_edited" {
		t.Fatalf("чужие сообщения не классифицированы: %#v", observations)
	}
}

// Проверочный файл от зарегистрированной точки распознаётся отдельно и не
// передаётся в обработку рабочих перехватов.
func TestServerReceiverRecognizesTransportProbe(t *testing.T) {
	t.Parallel()
	probeID := "018f22d2-3f5c-7abc-8123-0123456789ab"
	key := make([]byte, 32)
	sentAt := time.Now().UTC().Truncate(time.Second)
	probe := probePayload{
		Schema: probeSchema, ClientID: "textile", ProbeID: probeID,
		SentAt: sentAt.Format(time.RFC3339), Shift: application.EmptyShiftSnapshot(),
	}
	probe.MAC = probeMAC(key, probe.ClientID, probe.ProbeID, probe.SentAt, probe.Shift)
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": []any{map[string]any{
				"update_id": 8, "message": map[string]any{
					"message_id": 16, "message_thread_id": 9, "date": sentAt.Unix(),
					"from":     map[string]any{"id": 101, "is_bot": true, "username": "textile_bot"},
					"chat":     map[string]any{"id": -1001, "type": "supergroup"},
					"document": map[string]any{"file_id": "probe", "file_name": probeID + ".icprobe", "file_size": 100},
				},
			}}})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": map[string]any{"file_path": "probe.json"}})
		case strings.Contains(request.URL.Path, "/file/bot"):
			_ = json.NewEncoder(writer).Encode(probe)
		default:
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		}
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := ServerReceiver{
		API: client, ChatID: -1001, MaxBytes: 1024,
		Authorize: func(context.Context, application.IncomingPacket) (bool, error) { return true, nil },
	}
	packets, _, err := receiver.Packets(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(packets) != 1 || packets[0].ProbeID != probeID || packets[0].ReceivedAt.IsZero() || !VerifyProbe(packets[0], key) {
		t.Fatalf("проверка не распознана: %#v", packets)
	}
}

// Исчезнувший файл одного обновления не должен навсегда удерживать все более
// новые пакеты. Клиент не получит подтверждение и безопасно пришлёт его снова.
func TestServerReceiverSkipsPermanentlyUnavailableFile(t *testing.T) {
	t.Parallel()
	var deleted atomic.Int32
	var discarded atomic.Int64
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": []any{
				transportDocumentUpdate(21, 31, "gone"),
				transportDocumentUpdate(22, 32, "available"),
			}})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			var input struct {
				FileID string `json:"file_id"`
			}
			_ = json.NewDecoder(request.Body).Decode(&input)
			if input.FileID == "gone" {
				writer.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(writer).Encode(map[string]any{
					"ok": false, "error_code": 400, "description": "Bad Request: wrong file identifier",
				})
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"file_path": "available.icevent"},
			})
		case strings.Contains(request.URL.Path, "/file/bot"):
			_, _ = writer.Write([]byte("encrypted"))
		case strings.HasSuffix(request.URL.Path, "/deleteMessage"):
			deleted.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		default:
			http.Error(writer, "unexpected request", http.StatusNotFound)
		}
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := ServerReceiver{
		API: client, ChatID: -1001, MaxBytes: 1024,
		Authorize: func(context.Context, application.IncomingPacket) (bool, error) { return true, nil },
		Discard:   func(_ context.Context, messageID int64) { discarded.Store(messageID) },
	}
	packets, next, err := receiver.Packets(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(packets) != 1 || packets[0].MessageID != 32 || string(packets[0].Payload) != "encrypted" {
		t.Fatalf("следующий исправный пакет не получен: %#v", packets)
	}
	if next != 23 || discarded.Load() != 31 || deleted.Load() != 0 {
		t.Fatalf("неверное продвижение или очистка: next=%d discarded=%d deleted=%d",
			next, discarded.Load(), deleted.Load())
	}
}

// Ошибка сервера Telegram может исчезнуть при следующей попытке. В этом
// случае смещение не двигается и пакет не теряется.
func TestServerReceiverKeepsOffsetOnTemporaryFileFailure(t *testing.T) {
	t.Parallel()
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": []any{transportDocumentUpdate(41, 51, "temporary")},
			})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			writer.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": false, "error_code": 500, "description": "Internal Server Error",
			})
		default:
			http.Error(writer, "unexpected request", http.StatusNotFound)
		}
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := ServerReceiver{
		API: client, ChatID: -1001, MaxBytes: 1024,
		Authorize: func(context.Context, application.IncomingPacket) (bool, error) { return true, nil },
	}
	packets, next, err := receiver.Packets(context.Background(), 40)
	if err == nil || len(packets) != 0 || next != 40 {
		t.Fatalf("временная ошибка должна сохранить смещение: packets=%#v next=%d err=%v", packets, next, err)
	}
}

func TestPermanentTransportFileErrorClassification(t *testing.T) {
	t.Parallel()
	for _, err := range []error{
		&apiError{code: http.StatusBadRequest},
		&apiError{code: http.StatusNotFound},
		&fileDownloadError{code: http.StatusGone},
		errTelegramFileTooLarge,
	} {
		if !permanentTransportFileError(err) {
			t.Errorf("постоянная ошибка не распознана: %v", err)
		}
	}
	for _, err := range []error{
		&apiError{code: http.StatusTooManyRequests},
		&apiError{code: http.StatusInternalServerError},
		&fileDownloadError{code: http.StatusServiceUnavailable},
		context.DeadlineExceeded,
	} {
		if permanentTransportFileError(err) {
			t.Errorf("временная ошибка ошибочно признана постоянной: %v", err)
		}
	}
}

func transportDocumentUpdate(updateID, messageID int64, fileID string) map[string]any {
	return map[string]any{
		"update_id": updateID,
		"message": map[string]any{
			"message_id": messageID, "message_thread_id": 9, "date": time.Now().Unix(),
			"from":     map[string]any{"id": 101, "is_bot": true, "username": "station_bot"},
			"chat":     map[string]any{"id": -1001, "type": "supergroup"},
			"document": map[string]any{"file_id": fileID, "file_name": fileID + ".icevent", "file_size": 100},
		},
	}
}
