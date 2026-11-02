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
	"sync"
	"time"

	"intercept/internal/application"
)

const serverReceiverDownloadWorkers = 4

type serverReceiveDisposition uint8

const (
	receiveSkip serverReceiveDisposition = iota
	receiveMissingSender
	receiveNonTransport
	receiveOtherDocument
	receiveInvalidSize
	receiveUnauthorized
	receiveAuthorizationRetry
	receiveDownload
)

type serverReceiveItem struct {
	updateID    int64
	message     *Message
	edited      bool
	filename    string
	disposition serverReceiveDisposition
	packet      application.IncomingPacket
	err         error
	download    serverDownloadResult
}

type serverDownloadStage uint8

const (
	downloadGetFile serverDownloadStage = iota
	downloadFile
)

type serverDownloadResult struct {
	payload    []byte
	receivedAt time.Time
	stage      serverDownloadStage
	err        error
}

type serverDownloadJob struct {
	index int
	ctx   context.Context
}

type serverDownloadCompletion struct {
	index  int
	result serverDownloadResult
}

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
	// очистки. Ошибка запрещает продвигать Telegram offset. Без обработчика
	// сохраняется совместимый однократный вызов API.
	Discard func(context.Context, int64) error
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
	items := receiver.prepareReceiveItems(ctx, updates)
	receiver.downloadReceiveItems(ctx, items)
	return receiver.finishReceiveItems(ctx, items, offset)
}

// prepareReceiveItems разбирает метаданные строго в порядке Telegram и не
// скачивает содержимое до проверки зарегистрированного отправителя. При
// временной ошибке авторизации более новые обновления даже не планируются:
// безопасный префикс останется тем же, что и при последовательном приёме.
func (receiver *ServerReceiver) prepareReceiveItems(ctx context.Context, updates []Update) []serverReceiveItem {
	items := make([]serverReceiveItem, 0, len(updates))
	for _, update := range updates {
		item := serverReceiveItem{updateID: update.UpdateID}
		message := update.Message
		if message == nil {
			message = update.ChannelPost
		}
		if message == nil {
			message, item.edited = update.EditedMessage, update.EditedMessage != nil
		}
		if message == nil {
			message, item.edited = update.EditedChannelPost, update.EditedChannelPost != nil
		}
		item.message = message
		if message == nil || message.Chat.ID != receiver.ChatID {
			items = append(items, item)
			continue
		}
		if message.From == nil {
			item.disposition = receiveMissingSender
			items = append(items, item)
			continue
		}
		if message.Document == nil {
			item.disposition = receiveNonTransport
			items = append(items, item)
			continue
		}
		item.filename = strings.ToLower(message.Document.FileName)
		if !message.From.IsBot || (!strings.HasSuffix(item.filename, ".icevent") && !strings.HasSuffix(item.filename, ".icprobe")) {
			item.disposition = receiveOtherDocument
			items = append(items, item)
			continue
		}
		if message.Document.FileSize <= 0 || message.Document.FileSize > receiver.MaxBytes {
			item.disposition = receiveInvalidSize
			items = append(items, item)
			continue
		}
		item.packet = application.IncomingPacket{
			UpdateID:    update.UpdateID,
			SenderBotID: message.From.ID, SenderBotUsername: message.From.Username,
			ChatID: message.Chat.ID, ThreadID: message.MessageThreadID,
			MessageID: message.MessageID,
			SentAt:    time.Unix(message.Date, 0).UTC(),
		}
		if receiver.Authorize != nil {
			allowed, authorizeErr := receiver.Authorize(ctx, item.packet)
			if authorizeErr != nil {
				item.disposition = receiveAuthorizationRetry
				item.err = authorizeErr
				items = append(items, item)
				break
			}
			if !allowed {
				item.disposition = receiveUnauthorized
				items = append(items, item)
				continue
			}
		}
		item.disposition = receiveDownload
		items = append(items, item)
	}
	return items
}

// downloadReceiveItems ограничивает число одновременных пар getFile/download
// четырьмя. Временная ошибка отменяет только более новые загрузки: безопасный
// префикс завершается, а зависший suffix не задерживает возврат до HTTP timeout.
func (receiver *ServerReceiver) downloadReceiveItems(ctx context.Context, items []serverReceiveItem) {
	jobs := make(chan serverDownloadJob, len(items))
	results := make(chan serverDownloadCompletion, len(items))
	cancels := make(map[int]context.CancelFunc)
	jobCount := 0
	for index := range items {
		if items[index].disposition == receiveDownload {
			jobContext, cancel := context.WithCancel(ctx)
			cancels[index] = cancel
			jobs <- serverDownloadJob{index: index, ctx: jobContext}
			jobCount++
		}
	}
	close(jobs)
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()
	if jobCount == 0 {
		return
	}
	workerCount := min(serverReceiverDownloadWorkers, jobCount)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for job := range jobs {
				result := serverDownloadResult{stage: downloadGetFile}
				if ctxErr := job.ctx.Err(); ctxErr != nil {
					result.err = ctxErr
					results <- serverDownloadCompletion{index: job.index, result: result}
					continue
				}
				file, getFileErr := receiver.API.GetFile(job.ctx, items[job.index].message.Document.FileID)
				if getFileErr != nil {
					result.err = getFileErr
					results <- serverDownloadCompletion{index: job.index, result: result}
					continue
				}
				result.stage = downloadFile
				result.payload, result.err = receiver.API.DownloadFile(job.ctx, file.FilePath, receiver.MaxBytes)
				if result.err == nil {
					result.receivedAt = time.Now().UTC()
				}
				results <- serverDownloadCompletion{index: job.index, result: result}
			}
		}()
	}
	blockingIndex := len(items)
	for range jobCount {
		completion := <-results
		items[completion.index].download = completion.result
		if completion.index >= blockingIndex || completion.result.err == nil ||
			permanentTransportFileError(completion.result.err) || errors.Is(completion.result.err, context.Canceled) {
			continue
		}
		blockingIndex = completion.index
		for index, cancel := range cancels {
			if index > blockingIndex {
				cancel()
			}
		}
	}
	workers.Wait()
}

// finishReceiveItems применяет результаты в исходном порядке update_id. Даже
// если более новый файл успел скачаться первым, он не попадёт в packets и не
// продвинет offset через временную ошибку более старого обновления.
func (receiver *ServerReceiver) finishReceiveItems(ctx context.Context, items []serverReceiveItem, offset int64) ([]application.IncomingPacket, int64, error) {
	next := offset
	var packets []application.IncomingPacket
	for _, item := range items {
		// При ошибке этого обновления уже скачанный префикс можно сохранить
		// вместе со смещением до него, не пропуская проблемный пакет.
		processedUntil := next
		itemNext := next
		if item.updateID >= itemNext {
			itemNext = item.updateID + 1
		}
		message := item.message
		if item.disposition == receiveSkip {
			next = itemNext
			continue
		}
		observation := receiver.observation(item.updateID, message)
		switch item.disposition {
		case receiveMissingSender:
			observation.MessageKind, observation.Outcome, observation.Reason, observation.Rank =
				"unknown", "ignored_missing_sender", "Telegram не указал отправителя", 100
			receiver.observe(observation)
			next = itemNext
			continue
		case receiveNonTransport:
			observation.MessageKind = "user_media"
			if strings.TrimSpace(message.Text) != "" || strings.TrimSpace(message.Caption) != "" {
				observation.MessageKind = "user_text"
			}
			if message.From.IsBot {
				observation.MessageKind = "bot_non_transport"
			}
			if item.edited {
				observation.MessageKind += "_edited"
			}
			observation.Outcome, observation.Reason, observation.Rank =
				"ignored_non_transport", "Не является служебным пакетом EAVC Connect", 100
			receiver.observe(observation)
			next = itemNext
			continue
		case receiveOtherDocument:
			observation.MessageKind, observation.Outcome, observation.Reason, observation.Rank =
				"other_document", "ignored_non_transport", "Файл не является пакетом зарегистрированного бота EAVC Connect", 100
			if item.edited {
				observation.MessageKind = "other_document_edited"
			}
			receiver.observe(observation)
			next = itemNext
			continue
		}
		observation.MessageKind = "event_packet"
		if strings.HasSuffix(item.filename, ".icprobe") {
			observation.MessageKind = "probe_packet"
		}
		switch item.disposition {
		case receiveInvalidSize:
			observation.Outcome, observation.Reason, observation.Rank =
				"ignored_invalid_size", "Недопустимый размер служебного файла", 100
			receiver.observe(observation)
			// Размер уже известен из метаданных Telegram, поэтому такой пакет
			// окончательно невалиден и не должен навсегда оставаться в служебной
			// группе. Discard сохраняет неудавшееся удаление в долговечной очереди.
			if err := receiver.discardPacket(ctx, message.MessageID); err != nil {
				return packets, processedUntil, fmt.Errorf("поставить невалидный пакет в очередь очистки: %w", err)
			}
			next = itemNext
			continue
		case receiveUnauthorized:
			observation.Outcome, observation.Reason, observation.Rank =
				"ignored_unauthorized", "Бот или тема не зарегистрированы как рабочая точка", 100
			receiver.observe(observation)
			// Неизвестный бот не заставит сервер скачивать произвольные файлы.
			if err := receiver.discardPacket(ctx, message.MessageID); err != nil {
				return packets, processedUntil, fmt.Errorf("поставить пакет неизвестного бота в очередь очистки: %w", err)
			}
			next = itemNext
			continue
		case receiveAuthorizationRetry:
			observation.Outcome, observation.Reason, observation.Rank =
				"authorization_retry", "Временная ошибка проверки отправителя", 20
			receiver.observe(observation)
			return packets, processedUntil, fmt.Errorf("проверить отправителя пакета: %w", item.err)
		}
		packet := item.packet
		if item.download.err != nil {
			if permanentTransportFileError(item.download.err) {
				observation.Outcome, observation.Reason, observation.Rank =
					"unavailable_file", "Telegram больше не отдаёт служебный файл; Клиент повторит пакет", 100
				receiver.observe(observation)
				if err := receiver.skipUnavailableFile(ctx, packet, item.download.err); err != nil {
					return packets, processedUntil, fmt.Errorf("поставить недоступный пакет в очередь очистки: %w", err)
				}
				next = itemNext
				continue
			}
			reason := "Временная ошибка скачивания служебного файла"
			resultErr := item.download.err
			if item.download.stage == downloadGetFile {
				reason = "Временная ошибка получения служебного файла"
				resultErr = fmt.Errorf("получить пакет Telegram: %w", resultErr)
			}
			observation.Outcome, observation.Reason, observation.Rank =
				"download_retry", reason, 20
			receiver.observe(observation)
			return packets, processedUntil, resultErr
		}
		packet.Payload = item.download.payload
		packet.ReceivedAt = item.download.receivedAt
		if strings.HasSuffix(item.filename, ".icprobe") {
			decoder := json.NewDecoder(strings.NewReader(string(packet.Payload)))
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
				if err := receiver.discardPacket(ctx, message.MessageID); err != nil {
					return packets, processedUntil, fmt.Errorf("поставить повреждённый probe в очередь очистки: %w", err)
				}
				next = itemNext
				continue
			}
			probeAt, err := time.Parse(time.RFC3339, probe.SentAt)
			if err != nil {
				observation.Outcome, observation.Reason, observation.Rank =
					"invalid_probe", "Некорректное время контрольного пакета", 100
				receiver.observe(observation)
				if err := receiver.discardPacket(ctx, message.MessageID); err != nil {
					return packets, processedUntil, fmt.Errorf("поставить probe с неверным временем в очередь очистки: %w", err)
				}
				next = itemNext
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
				if err := receiver.discardPacket(ctx, message.MessageID); err != nil {
					return packets, processedUntil, fmt.Errorf("поставить неподлинный пакет в очередь очистки: %w", err)
				}
				next = itemNext
				continue
			}
		}
		observation.Outcome, observation.Reason, observation.Rank =
			"awaiting_processing", "Служебный пакет передан обработчику", 10
		receiver.observe(observation)
		packets = append(packets, packet)
		next = itemNext
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

func (receiver *ServerReceiver) skipUnavailableFile(ctx context.Context, packet application.IncomingPacket, err error) error {
	if discardErr := receiver.discardPacket(ctx, packet.MessageID); discardErr != nil {
		return discardErr
	}
	if receiver.Logger != nil {
		receiver.Logger.Warn(
			"недоступный транспортный файл пропущен; рабочая точка повторит отправку",
			"sender_bot", packet.SenderBotUsername,
			"message_id", packet.MessageID,
			"error", err,
		)
	}
	return nil
}

func (receiver *ServerReceiver) discardPacket(ctx context.Context, messageID int64) error {
	if receiver.Discard != nil {
		return receiver.Discard(ctx, messageID)
	}
	if err := receiver.DeletePacket(ctx, messageID); err != nil {
		if receiver.Logger != nil && ctx.Err() == nil {
			receiver.Logger.Warn("не удалось удалить отброшенный транспортный файл",
				"telegram_message_id", messageID, "error", err)
		}
		return err
	}
	return nil
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
