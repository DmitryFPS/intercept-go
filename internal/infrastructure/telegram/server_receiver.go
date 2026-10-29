package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"intercept/internal/application"
)

// ServerReceiver получает только файлы из настроенной транспортной группы.
type ServerReceiver struct {
	API      *Client
	ChatID   int64
	MaxBytes int64
	Logger   *slog.Logger
	// Authorize проверяет метаданные отправителя до скачивания файла.
	Authorize func(context.Context, application.IncomingPacket) (bool, error)
	// Authenticate проверяет подпись содержимого после скачивания, но до
	// долговечной записи. Временная ошибка оставляет Telegram offset прежним.
	Authenticate func(context.Context, application.IncomingPacket) error
	// Discard передаёт удаление служебного файла владельцу долговечной очереди
	// очистки. Без обработчика сохраняется совместимый однократный вызов API.
	Discard func(context.Context, int64)
	// Observe неблокирующе передаёт техническую классификацию владельцу журнала.
	// Ошибка журнала не должна менять приём или продвижение Telegram offset.
	Observe func(application.TransportObservation)
}

// Packets возвращает пакеты и следующее безопасное смещение Telegram.
func (receiver *ServerReceiver) Packets(ctx context.Context, offset int64) ([]application.IncomingPacket, int64, error) {
	updates, err := receiver.API.GetUpdates(ctx, offset, 25)
	if err != nil {
		return nil, offset, err
	}
	next := offset
	var packets []application.IncomingPacket
	for _, update := range updates {
		// При ошибке этого обновления уже скачанный префикс можно сохранить
		// вместе со смещением до него, не пропуская проблемный пакет.
		processedUntil := next
		if update.UpdateID >= next {
			next = update.UpdateID + 1
		}
		message := update.Message
		edited := false
		if message == nil {
			message = update.ChannelPost
		}
		if message == nil {
			message, edited = update.EditedMessage, update.EditedMessage != nil
		}
		if message == nil {
			message, edited = update.EditedChannelPost, update.EditedChannelPost != nil
		}
		if message == nil || message.Chat.ID != receiver.ChatID {
			continue
		}
		observation := receiver.observation(update.UpdateID, message)
		if message.From == nil {
			observation.MessageKind, observation.Outcome, observation.Reason, observation.Rank =
				"unknown", "ignored_missing_sender", "Telegram не указал отправителя", 100
			receiver.observe(observation)
			continue
		}
		if message.Document == nil {
			observation.MessageKind = "user_media"
			if strings.TrimSpace(message.Text) != "" || strings.TrimSpace(message.Caption) != "" {
				observation.MessageKind = "user_text"
			}
			if message.From.IsBot {
				observation.MessageKind = "bot_non_transport"
			}
			if edited {
				observation.MessageKind += "_edited"
			}
			observation.Outcome, observation.Reason, observation.Rank =
				"ignored_non_transport", "Не является служебным пакетом EAVC Connect", 100
			receiver.observe(observation)
			continue
		}
		filename := strings.ToLower(message.Document.FileName)
		if !message.From.IsBot || (!strings.HasSuffix(filename, ".icevent") && !strings.HasSuffix(filename, ".icprobe")) {
			observation.MessageKind, observation.Outcome, observation.Reason, observation.Rank =
				"other_document", "ignored_non_transport", "Файл не является пакетом зарегистрированного бота EAVC Connect", 100
			if edited {
				observation.MessageKind = "other_document_edited"
			}
			receiver.observe(observation)
			continue
		}
		observation.MessageKind = "event_packet"
		if strings.HasSuffix(filename, ".icprobe") {
			observation.MessageKind = "probe_packet"
		}
		if message.Document.FileSize <= 0 || message.Document.FileSize > receiver.MaxBytes {
			observation.Outcome, observation.Reason, observation.Rank =
				"ignored_invalid_size", "Недопустимый размер служебного файла", 100
			receiver.observe(observation)
			// Размер уже известен из метаданных Telegram, поэтому такой пакет
			// окончательно невалиден и не должен навсегда оставаться в служебной
			// группе. Discard сохраняет неудавшееся удаление в долговечной очереди.
			receiver.discardPacket(ctx, message.MessageID)
			continue
		}
		packet := application.IncomingPacket{
			UpdateID:    update.UpdateID,
			SenderBotID: message.From.ID, SenderBotUsername: message.From.Username,
			ChatID: message.Chat.ID, ThreadID: message.MessageThreadID,
			MessageID: message.MessageID,
			SentAt:    time.Unix(message.Date, 0).UTC(),
		}
		if receiver.Authorize != nil {
			allowed, err := receiver.Authorize(ctx, packet)
			if err != nil {
				observation.Outcome, observation.Reason, observation.Rank =
					"authorization_retry", "Временная ошибка проверки отправителя", 20
				receiver.observe(observation)
				return packets, processedUntil, fmt.Errorf("проверить отправителя пакета: %w", err)
			}
			if !allowed {
				observation.Outcome, observation.Reason, observation.Rank =
					"ignored_unauthorized", "Бот или тема не зарегистрированы как рабочая точка", 100
				receiver.observe(observation)
				// Неизвестный бот не заставит сервер скачивать произвольные файлы.
				receiver.discardPacket(ctx, message.MessageID)
				continue
			}
		}
		file, err := receiver.API.GetFile(ctx, message.Document.FileID)
		if err != nil {
			if permanentTransportFileError(err) {
				observation.Outcome, observation.Reason, observation.Rank =
					"unavailable_file", "Telegram больше не отдаёт служебный файл; Клиент повторит пакет", 100
				receiver.observe(observation)
				receiver.skipUnavailableFile(ctx, packet, err)
				continue
			}
			observation.Outcome, observation.Reason, observation.Rank =
				"download_retry", "Временная ошибка получения служебного файла", 20
			receiver.observe(observation)
			return packets, processedUntil, fmt.Errorf("получить пакет Telegram: %w", err)
		}
		payload, err := receiver.API.DownloadFile(ctx, file.FilePath, receiver.MaxBytes)
		if err != nil {
			if permanentTransportFileError(err) {
				observation.Outcome, observation.Reason, observation.Rank =
					"unavailable_file", "Telegram больше не отдаёт служебный файл; Клиент повторит пакет", 100
				receiver.observe(observation)
				receiver.skipUnavailableFile(ctx, packet, err)
				continue
			}
			observation.Outcome, observation.Reason, observation.Rank =
				"download_retry", "Временная ошибка скачивания служебного файла", 20
			receiver.observe(observation)
			return packets, processedUntil, err
		}
		packet.Payload = payload
		packet.ReceivedAt = time.Now().UTC()
		if strings.HasSuffix(filename, ".icprobe") {
			decoder := json.NewDecoder(strings.NewReader(string(payload)))
			decoder.DisallowUnknownFields()
			var probe probePayload
			decodeErr := decoder.Decode(&probe)
			var trailing any
			trailingErr := decoder.Decode(&trailing)
			if decodeErr != nil || !errors.Is(trailingErr, io.EOF) || probe.Schema != probeSchema ||
				strings.TrimSpace(probe.ClientID) == "" || !validProbeID(probe.ProbeID) ||
				strings.TrimSpace(probe.MAC) == "" || application.ValidateShiftSnapshot(probe.Shift) != nil {
				observation.Outcome, observation.Reason, observation.Rank =
					"invalid_probe", "Повреждённый или неподписанный контрольный пакет", 100
				receiver.observe(observation)
				receiver.discardPacket(ctx, message.MessageID)
				continue
			}
			probeAt, err := time.Parse(time.RFC3339, probe.SentAt)
			if err != nil {
				observation.Outcome, observation.Reason, observation.Rank =
					"invalid_probe", "Некорректное время контрольного пакета", 100
				receiver.observe(observation)
				receiver.discardPacket(ctx, message.MessageID)
				continue
			}
			packet.ProbeID = probe.ProbeID
			packet.ProbeClientID = probe.ClientID
			packet.ProbeSentAt = probeAt.UTC()
			packet.ProbeMAC = probe.MAC
			packet.ProbeShift = application.NormalizeShiftSnapshot(probe.Shift)
		}
		if receiver.Authenticate != nil {
			if err := receiver.Authenticate(ctx, packet); err != nil {
				if !application.IsPermanentPacketError(err) {
					observation.Outcome, observation.Reason, observation.Rank =
						"authentication_retry", "Временная ошибка проверки содержимого пакета", 20
					receiver.observe(observation)
					return packets, processedUntil, fmt.Errorf("проверить содержимое пакета: %w", err)
				}
				observation.Outcome, observation.Reason, observation.Rank =
					"rejected_authentication", "Служебный пакет не прошёл проверку подлинности", 200
				receiver.observe(observation)
				receiver.discardPacket(ctx, message.MessageID)
				continue
			}
		}
		observation.Outcome, observation.Reason, observation.Rank =
			"awaiting_processing", "Служебный пакет передан обработчику", 10
		receiver.observe(observation)
		packets = append(packets, packet)
	}
	return packets, next, nil
}

func (receiver *ServerReceiver) observation(updateID int64, message *Message) application.TransportObservation {
	item := application.TransportObservation{
		UpdateID: updateID, MessageID: message.MessageID, ChatID: message.Chat.ID,
		ThreadID: message.MessageThreadID, SentAt: time.Unix(message.Date, 0).UTC(),
		ObservedAt: time.Now().UTC(),
	}
	if message.From != nil {
		item.SenderBotID = message.From.ID
		item.SenderBotUsername = message.From.Username
	}
	return item
}

func (receiver *ServerReceiver) observe(item application.TransportObservation) {
	if receiver.Observe != nil {
		receiver.Observe(item)
	}
}

func permanentTransportFileError(err error) bool {
	if errors.Is(err, errTelegramFileTooLarge) {
		return true
	}
	var telegramError *apiError
	if errors.As(err, &telegramError) {
		return telegramError.code == http.StatusBadRequest || telegramError.code == http.StatusNotFound
	}
	var downloadError *fileDownloadError
	if errors.As(err, &downloadError) {
		return downloadError.code == http.StatusBadRequest || downloadError.code == http.StatusNotFound ||
			downloadError.code == http.StatusGone
	}
	return false
}

func (receiver *ServerReceiver) skipUnavailableFile(ctx context.Context, packet application.IncomingPacket, err error) {
	if receiver.Logger != nil {
		receiver.Logger.Warn(
			"недоступный транспортный файл пропущен; рабочая точка повторит отправку",
			"sender_bot", packet.SenderBotUsername,
			"message_id", packet.MessageID,
			"error", err,
		)
	}
	receiver.discardPacket(ctx, packet.MessageID)
}

func (receiver *ServerReceiver) discardPacket(ctx context.Context, messageID int64) {
	if receiver.Discard != nil {
		receiver.Discard(ctx, messageID)
		return
	}
	if err := receiver.DeletePacket(ctx, messageID); err != nil && receiver.Logger != nil && ctx.Err() == nil {
		receiver.Logger.Warn("не удалось удалить отброшенный транспортный файл",
			"telegram_message_id", messageID, "error", err)
	}
}

func validProbeID(value string) bool {
	value = strings.TrimSpace(value)
	return len(value) >= 32 && len(value) <= 64 && !strings.ContainsAny(value, " \r\n\t")
}

// DeletePacket удаляет уже сохранённый транспортный файл из служебной группы.
// Ошибка удаления не влияет на приём: локальная дедупликация остаётся источником истины.
func (receiver *ServerReceiver) DeletePacket(ctx context.Context, messageID int64) error {
	if receiver.API == nil || receiver.ChatID == 0 || messageID <= 0 {
		return errors.New("не заданы данные транспортного сообщения")
	}
	return receiver.API.DeleteMessage(ctx, receiver.ChatID, messageID)
}

// Acknowledge сообщает боту точки, что событие записано сервером.
func (receiver *ServerReceiver) Acknowledge(ctx context.Context, botUsername, eventID, status string, key []byte, messageID int64, traceV4 bool) error {
	if strings.TrimSpace(botUsername) == "" || strings.TrimSpace(eventID) == "" {
		return errors.New("для подтверждения не задан бот или событие")
	}
	if status != "accepted" && status != "expired" {
		return errors.New("для подтверждения задан неизвестный статус")
	}
	if len(key) != 32 {
		return errors.New("для подтверждения не задан ключ рабочей точки")
	}
	ackText := AcknowledgementText(eventID, status, key)
	if traceV4 {
		if messageID <= 0 {
			return errors.New("для подтверждения трассы не задан ID транспортного сообщения")
		}
		ackText = AcknowledgementTraceText(eventID, status, messageID, key)
	}
	_, err := receiver.API.SendMessage(ctx, 0, botUsername, ackText)
	return err
}

// AcknowledgeProbe завершает двустороннюю проверку Bot-to-Bot без постановки
// тестового сообщения в рабочие маршруты.
func (receiver *ServerReceiver) AcknowledgeProbe(ctx context.Context, botUsername, probeID string, key []byte) error {
	if strings.TrimSpace(botUsername) == "" || !validProbeID(probeID) {
		return errors.New("для проверки связи не задан бот или идентификатор")
	}
	if len(key) != 32 {
		return errors.New("для проверки связи не задан ключ рабочей точки")
	}
	_, err := receiver.API.SendMessage(ctx, 0, botUsername, probeAcknowledgementText(probeID, key))
	return err
}
