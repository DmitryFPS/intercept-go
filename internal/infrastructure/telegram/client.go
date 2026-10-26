package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultBaseURL = "https://api.telegram.org"

var errTelegramFileTooLarge = errors.New("файл Telegram превышает допустимый размер")

const (
	// Telegram рекомендует не превышать примерно 30 сообщений в секунду на
	// бота, одно сообщение в секунду в одном чате и 20 сообщений в минуту в
	// группе. Используем небольшой запас, чтобы штатная очередь не получала 429.
	globalSendInterval  = 40 * time.Millisecond
	privateChatInterval = time.Second
	groupChatInterval   = 3 * time.Second
)

// Client — минимальный строго типизированный клиент Telegram Bot API.
type Client struct {
	token      string
	baseURL    string
	httpClient *http.Client
	sends      *sendLimiter
}

type sendLimiter struct {
	mu              sync.Mutex
	nextGlobal      time.Time
	nextByChat      map[int64]time.Time
	blockedUntil    time.Time
	globalInterval  time.Duration
	privateInterval time.Duration
	groupInterval   time.Duration
}

func newSendLimiter() *sendLimiter {
	return &sendLimiter{
		nextByChat: make(map[int64]time.Time), globalInterval: globalSendInterval,
		privateInterval: privateChatInterval, groupInterval: groupChatInterval,
	}
}

func (limiter *sendLimiter) wait(ctx context.Context, chatID int64) error {
	limiter.mu.Lock()
	now := time.Now()
	scheduled := now
	if limiter.blockedUntil.After(scheduled) {
		scheduled = limiter.blockedUntil
	}
	if limiter.nextGlobal.After(scheduled) {
		scheduled = limiter.nextGlobal
	}
	if next := limiter.nextByChat[chatID]; next.After(scheduled) {
		scheduled = next
	}
	chatInterval := limiter.privateInterval
	if chatID < 0 {
		chatInterval = limiter.groupInterval
	}
	limiter.nextGlobal = scheduled.Add(limiter.globalInterval)
	limiter.nextByChat[chatID] = scheduled.Add(chatInterval)
	limiter.mu.Unlock()

	for {
		limiter.mu.Lock()
		if limiter.blockedUntil.After(scheduled) {
			scheduled = limiter.blockedUntil
		}
		limiter.mu.Unlock()
		delay := time.Until(scheduled)
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (limiter *sendLimiter) blockFor(delay time.Duration) {
	if delay <= 0 {
		return
	}
	until := time.Now().Add(delay)
	limiter.mu.Lock()
	if until.After(limiter.blockedUntil) {
		limiter.blockedUntil = until
	}
	limiter.mu.Unlock()
}

// rateLimitError сообщает прикладной очереди точную паузу, которую вернул
// Telegram. Очередь переживает перезапуск, поэтому ожидание не теряется.
type rateLimitError struct {
	after       time.Duration
	description string
}

func (err *rateLimitError) Error() string {
	return fmt.Sprintf("Telegram временно ограничил частоту запросов; повтор не раньше чем через %s: %s",
		err.after, err.description)
}

func (err *rateLimitError) RetryAfter() time.Duration { return err.after }

// apiError сохраняет код Telegram Bot API для надёжной классификации. Текст
// остаётся человекочитаемым и совместимым с существующими журналами.
type apiError struct {
	code        int
	description string
}

func (err *apiError) Error() string {
	return fmt.Sprintf("telegram: %s (код %d)", err.description, err.code)
}

// fileDownloadError сохраняет HTTP-код скачивания. Получатель транспорта
// отличает исчезнувший файл от временной ошибки сети и не удерживает за ним
// все более новые обновления Telegram.
type fileDownloadError struct {
	code int
}

func (err *fileDownloadError) Error() string {
	return fmt.Sprintf("telegram вернул HTTP %d при скачивании файла", err.code)
}

// User содержит только поля Telegram-пользователя, нужные транспорту.
type User struct {
	ID       int64  `json:"id"`
	IsBot    bool   `json:"is_bot"`
	Username string `json:"username"`
}

// Chat представляет группу, канал или личный диалог Telegram.
type Chat struct {
	ID    int64  `json:"id"`
	Type  string `json:"type"`
	Title string `json:"title"`
}

// Document описывает файл до его безопасного скачивания.
type Document struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MimeType string `json:"mime_type"`
	FileSize int64  `json:"file_size"`
}

// Message содержит используемую часть сообщения Telegram.
type Message struct {
	MessageID       int64     `json:"message_id"`
	MessageThreadID int64     `json:"message_thread_id"`
	Date            int64     `json:"date"`
	From            *User     `json:"from"`
	Chat            Chat      `json:"chat"`
	Text            string    `json:"text"`
	Caption         string    `json:"caption"`
	Document        *Document `json:"document"`
}

// Update объединяет обычные сообщения и публикации каналов.
type Update struct {
	UpdateID    int64    `json:"update_id"`
	Message     *Message `json:"message"`
	ChannelPost *Message `json:"channel_post"`
}

// File содержит путь, выданный Telegram после проверки file_id.
type File struct {
	FileID   string `json:"file_id"`
	FileSize int64  `json:"file_size"`
	FilePath string `json:"file_path"`
}

// New создаёт клиент официального Telegram Bot API.
func New(token string, httpClient *http.Client) (*Client, error) {
	return NewWithBaseURL(token, defaultBaseURL, httpClient)
}

// NewWithBaseURL позволяет тестам подменить Telegram локальным HTTP-сервером.
func NewWithBaseURL(token, baseURL string, httpClient *http.Client) (*Client, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("не задан токен Telegram-бота")
	}
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, errors.New("некорректный адрес Telegram API")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 70 * time.Second}
	}
	return &Client{
		token: token, baseURL: strings.TrimRight(baseURL, "/"), httpClient: httpClient,
		sends: newSendLimiter(),
	}, nil
}

// GetMe проверяет токен и возвращает неизменяемый ID бота.
func (client *Client) GetMe(ctx context.Context) (User, error) {
	var user User
	if err := client.callJSON(ctx, "getMe", nil, &user); err != nil {
		return User{}, err
	}
	return user, nil
}

// DeleteWebhook освобождает Bot API для получения обновлений методом polling.
func (client *Client) DeleteWebhook(ctx context.Context, dropPending bool) error {
	return client.callJSON(ctx, "deleteWebhook", map[string]any{"drop_pending_updates": dropPending}, nil)
}

// SendDocument загружает ограниченный программой служебный файл в группу.
func (client *Client) SendDocument(ctx context.Context, chatID, threadID int64, filename string, payload []byte, caption string) (Message, error) {
	if err := client.sends.wait(ctx, chatID); err != nil {
		return Message{}, err
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	writer.WriteField("chat_id", strconv.FormatInt(chatID, 10))
	if threadID > 0 {
		writer.WriteField("message_thread_id", strconv.FormatInt(threadID, 10))
	}
	if caption != "" {
		writer.WriteField("caption", caption)
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="document"; filename=%q`, filename))
	header.Set("Content-Type", "application/octet-stream")
	part, err := writer.CreatePart(header)
	if err != nil {
		return Message{}, fmt.Errorf("создать Telegram-документ: %w", err)
	}
	if _, err := part.Write(payload); err != nil {
		return Message{}, fmt.Errorf("записать Telegram-документ: %w", err)
	}
	if err := writer.Close(); err != nil {
		return Message{}, fmt.Errorf("завершить Telegram-документ: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.methodURL("sendDocument"), &body)
	if err != nil {
		return Message{}, fmt.Errorf("создать запрос sendDocument: %w", err)
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	var message Message
	if err := client.do(request, &message); err != nil {
		return Message{}, err
	}
	return message, nil
}

// SendMessage отправляет личное машинное подтверждение указанному боту.
func (client *Client) SendMessage(ctx context.Context, chatID int64, username, text string) (Message, error) {
	limiterID := chatID
	if strings.TrimSpace(username) != "" {
		limiterID = privateRecipientLimiterID(username)
	}
	if err := client.sends.wait(ctx, limiterID); err != nil {
		return Message{}, err
	}
	target := any(chatID)
	if strings.TrimSpace(username) != "" {
		target = "@" + strings.TrimPrefix(strings.TrimSpace(username), "@")
	}
	var message Message
	err := client.callJSON(ctx, "sendMessage", map[string]any{"chat_id": target, "text": text}, &message)
	return message, err
}

// privateRecipientLimiterID не позволяет ответам разным ботам ошибочно делить
// одно ограничение личного диалога. Общий лимит бота при этом остаётся единым.
func privateRecipientLimiterID(username string) int64 {
	digest := fnv.New64a()
	_, _ = digest.Write([]byte(strings.ToLower(strings.TrimPrefix(strings.TrimSpace(username), "@"))))
	value := int64(digest.Sum64() & ((1 << 63) - 1))
	if value == 0 {
		return 1
	}
	return value
}

// SendText публикует текст в указанную группу и тему.
func (client *Client) SendText(ctx context.Context, chatID, threadID int64, text string) (Message, error) {
	if err := client.sends.wait(ctx, chatID); err != nil {
		return Message{}, err
	}
	request := map[string]any{"chat_id": chatID, "text": text}
	if threadID > 0 {
		request["message_thread_id"] = threadID
	}
	var message Message
	err := client.callJSON(ctx, "sendMessage", request, &message)
	return message, err
}

// SendHTML публикует подготовленное сообщение в формате Telegram HTML.
func (client *Client) SendHTML(ctx context.Context, chatID, threadID int64, text string) (Message, error) {
	if err := client.sends.wait(ctx, chatID); err != nil {
		return Message{}, err
	}
	request := map[string]any{
		"chat_id": chatID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": true,
	}
	if threadID > 0 {
		request["message_thread_id"] = threadID
	}
	var message Message
	err := client.callJSON(ctx, "sendMessage", request, &message)
	return message, err
}

// EditHTML исправляет ранее опубликованный временной блок без создания дубля.
func (client *Client) EditHTML(ctx context.Context, chatID, messageID int64, text string) error {
	if err := client.sends.wait(ctx, chatID); err != nil {
		return err
	}
	return client.callJSON(ctx, "editMessageText", map[string]any{
		"chat_id": chatID, "message_id": messageID, "text": text,
		"parse_mode": "HTML", "disable_web_page_preview": true,
	}, nil)
}

// DeleteMessage удаляет обработанный служебный либо проверочный материал.
func (client *Client) DeleteMessage(ctx context.Context, chatID, messageID int64) error {
	if err := client.sends.wait(ctx, chatID); err != nil {
		return err
	}
	return client.callJSON(ctx, "deleteMessage", map[string]any{
		"chat_id": chatID, "message_id": messageID,
	}, nil)
}

// TestTopic проверяет право записи и обязательно удаляет проверочное сообщение.
// Настройка не считается завершённой, если у бота нет права удаления: иначе
// служебный текст останется в рабочей группе.
func (client *Client) TestTopic(ctx context.Context, chatID, threadID int64) error {
	message, err := client.SendText(ctx, chatID, threadID, "Проверка подключения EAVC Connect. Сообщение будет удалено автоматически.")
	if err != nil {
		return err
	}
	if err := client.DeleteMessage(ctx, chatID, message.MessageID); err != nil {
		return fmt.Errorf("проверочное сообщение отправлено, но не удалено; удалите его вручную и выдайте боту право удаления: %w", err)
	}
	return nil
}

// GetUpdates читает очередную порцию обновлений с долговременным смещением.
func (client *Client) GetUpdates(ctx context.Context, offset int64, timeoutSeconds int) ([]Update, error) {
	var updates []Update
	err := client.callJSON(ctx, "getUpdates", map[string]any{
		"offset": offset, "timeout": timeoutSeconds, "allowed_updates": []string{"message", "channel_post"},
	}, &updates)
	return updates, err
}

// GetFile получает проверяемый Telegram-путь служебного файла.
func (client *Client) GetFile(ctx context.Context, fileID string) (File, error) {
	var file File
	if err := client.callJSON(ctx, "getFile", map[string]any{"file_id": fileID}, &file); err != nil {
		return File{}, err
	}
	return file, nil
}

// DownloadFile скачивает файл с жёстким ограничением размера.
func (client *Client) DownloadFile(ctx context.Context, filePath string, maxBytes int64) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		client.baseURL+"/file/bot"+client.token+"/"+strings.TrimLeft(filePath, "/"), nil)
	if err != nil {
		return nil, fmt.Errorf("создать запрос файла Telegram: %w", err)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("скачать файл Telegram: %s", client.safeError(err))
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, &fileDownloadError{code: response.StatusCode}
	}
	limited := io.LimitReader(response.Body, maxBytes+1)
	payload, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("прочитать файл Telegram: %w", err)
	}
	if int64(len(payload)) > maxBytes {
		return nil, errTelegramFileTooLarge
	}
	return payload, nil
}

type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	Description string          `json:"description"`
	ErrorCode   int             `json:"error_code"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

func (client *Client) callJSON(ctx context.Context, method string, input any, output any) error {
	payload, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("кодировать запрос %s: %w", method, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.methodURL(method), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("создать запрос %s: %w", method, err)
	}
	request.Header.Set("Content-Type", "application/json")
	return client.do(request, output)
}

func (client *Client) do(request *http.Request, output any) error {
	response, err := client.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("telegram %s: %s", methodName(request.URL.Path), client.safeError(err))
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 4*1024*1024))
	if err != nil {
		return errors.New("не удалось прочитать ответ Telegram")
	}
	var result apiResponse
	if err := json.Unmarshal(payload, &result); err != nil {
		return fmt.Errorf("telegram вернул некорректный ответ, HTTP %d", response.StatusCode)
	}
	if response.StatusCode != http.StatusOK || !result.OK {
		description := strings.TrimSpace(result.Description)
		if description == "" {
			description = "неизвестная ошибка"
		}
		if response.StatusCode == http.StatusTooManyRequests || result.ErrorCode == http.StatusTooManyRequests {
			after := time.Duration(result.Parameters.RetryAfter) * time.Second
			if after <= 0 {
				after = 5 * time.Second
			}
			client.sends.blockFor(after + time.Second)
			return &rateLimitError{after: after, description: description}
		}
		code := result.ErrorCode
		if code == 0 {
			code = response.StatusCode
		}
		return &apiError{code: code, description: description}
	}
	if output != nil && len(result.Result) != 0 {
		if err := json.Unmarshal(result.Result, output); err != nil {
			return errors.New("результат Telegram имеет неожиданный формат")
		}
	}
	return nil
}

func (client *Client) safeError(err error) string {
	if err == nil {
		return "неизвестная ошибка"
	}
	return strings.ReplaceAll(err.Error(), client.token, "***")
}

func (client *Client) methodURL(method string) string {
	return client.baseURL + "/bot" + client.token + "/" + method
}

func methodName(path string) string {
	if index := strings.LastIndex(path, "/"); index >= 0 {
		return path[index+1:]
	}
	return path
}
