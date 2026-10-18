package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.telegram.org"

// Client — минимальный строго типизированный клиент Telegram Bot API.
type Client struct {
	token      string
	baseURL    string
	httpClient *http.Client
}

type User struct {
	ID       int64  `json:"id"`
	IsBot    bool   `json:"is_bot"`
	Username string `json:"username"`
}

type Chat struct {
	ID    int64  `json:"id"`
	Type  string `json:"type"`
	Title string `json:"title"`
}

type Document struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	MimeType string `json:"mime_type"`
	FileSize int64  `json:"file_size"`
}

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

type Update struct {
	UpdateID    int64    `json:"update_id"`
	Message     *Message `json:"message"`
	ChannelPost *Message `json:"channel_post"`
}

type File struct {
	FileID   string `json:"file_id"`
	FileSize int64  `json:"file_size"`
	FilePath string `json:"file_path"`
}

func New(token string, httpClient *http.Client) (*Client, error) {
	return NewWithBaseURL(token, defaultBaseURL, httpClient)
}

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
	return &Client{token: token, baseURL: strings.TrimRight(baseURL, "/"), httpClient: httpClient}, nil
}

func (client *Client) GetMe(ctx context.Context) (User, error) {
	var user User
	if err := client.callJSON(ctx, "getMe", nil, &user); err != nil {
		return User{}, err
	}
	return user, nil
}

func (client *Client) DeleteWebhook(ctx context.Context, dropPending bool) error {
	return client.callJSON(ctx, "deleteWebhook", map[string]any{"drop_pending_updates": dropPending}, nil)
}

func (client *Client) SendDocument(ctx context.Context, chatID, threadID int64, filename string, payload []byte, caption string) (Message, error) {
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

func (client *Client) SendMessage(ctx context.Context, chatID int64, username, text string) (Message, error) {
	target := any(chatID)
	if strings.TrimSpace(username) != "" {
		target = "@" + strings.TrimPrefix(strings.TrimSpace(username), "@")
	}
	var message Message
	err := client.callJSON(ctx, "sendMessage", map[string]any{"chat_id": target, "text": text}, &message)
	return message, err
}

// SendText публикует текст в указанную группу и тему.
func (client *Client) SendText(ctx context.Context, chatID, threadID int64, text string) (Message, error) {
	request := map[string]any{"chat_id": chatID, "text": text}
	if threadID > 0 {
		request["message_thread_id"] = threadID
	}
	var message Message
	err := client.callJSON(ctx, "sendMessage", request, &message)
	return message, err
}

func (client *Client) GetUpdates(ctx context.Context, offset int64, timeoutSeconds int) ([]Update, error) {
	var updates []Update
	err := client.callJSON(ctx, "getUpdates", map[string]any{
		"offset": offset, "timeout": timeoutSeconds, "allowed_updates": []string{"message", "channel_post"},
	}, &updates)
	return updates, err
}

func (client *Client) GetFile(ctx context.Context, fileID string) (File, error) {
	var file File
	if err := client.callJSON(ctx, "getFile", map[string]any{"file_id": fileID}, &file); err != nil {
		return File{}, err
	}
	return file, nil
}

func (client *Client) DownloadFile(ctx context.Context, filePath string, maxBytes int64) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		client.baseURL+"/file/bot"+client.token+"/"+strings.TrimLeft(filePath, "/"), nil)
	if err != nil {
		return nil, fmt.Errorf("создать запрос файла Telegram: %w", err)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("скачать файл Telegram: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Telegram вернул HTTP %d при скачивании файла", response.StatusCode)
	}
	limited := io.LimitReader(response.Body, maxBytes+1)
	payload, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("прочитать файл Telegram: %w", err)
	}
	if int64(len(payload)) > maxBytes {
		return nil, errors.New("файл Telegram превышает допустимый размер")
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
		return fmt.Errorf("Telegram %s: %w", methodName(request.URL.Path), err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 4*1024*1024))
	if err != nil {
		return errors.New("не удалось прочитать ответ Telegram")
	}
	var result apiResponse
	if err := json.Unmarshal(payload, &result); err != nil {
		return fmt.Errorf("Telegram вернул некорректный ответ, HTTP %d", response.StatusCode)
	}
	if response.StatusCode != http.StatusOK || !result.OK {
		description := strings.TrimSpace(result.Description)
		if description == "" {
			description = "неизвестная ошибка"
		}
		return fmt.Errorf("Telegram: %s (код %d)", description, result.ErrorCode)
	}
	if output != nil && len(result.Result) != 0 {
		if err := json.Unmarshal(result.Result, output); err != nil {
			return errors.New("результат Telegram имеет неожиданный формат")
		}
	}
	return nil
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
