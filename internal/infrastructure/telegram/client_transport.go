package telegram

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"intercept/internal/application"
	"intercept/internal/protocol"
)

const (
	acknowledgementPrefixV3    = "IC-ACK/3 "
	acknowledgementPrefixV4    = "IC-ACK/4 "
	probeAcknowledgementPrefix = "IC-PROBE-ACK/2 "
	probeSchema                = "IC-PROBE/2"
	acknowledgementSchema      = "IC-ACK/3"
	acknowledgementSchemaV4    = "IC-ACK/4"
	probeAcknowledgementSchema = "IC-PROBE-ACK/2"
)

// ClientTransport передаёт конверты точки и принимает подтверждения сервера.
type ClientTransport struct {
	API               *Client
	ChatID            int64
	ThreadID          int64
	ServerBotID       int64
	ServerBotUsername string
	Key               []byte
}

type probePayload struct {
	Schema   string                    `json:"schema"`
	ClientID string                    `json:"client_id"`
	ProbeID  string                    `json:"probe_id"`
	SentAt   string                    `json:"sent_at"`
	Shift    application.ShiftSnapshot `json:"shift"`
	MAC      string                    `json:"mac"`
}

// SendProbe отправляет безопасный служебный файл, который сервер подтверждает
// напрямую клиентскому боту. Конечные группы такой файл никогда не получают.
func (transport *ClientTransport) SendProbe(ctx context.Context, clientID string, shift application.ShiftSnapshot) (string, error) {
	if len(transport.Key) != 32 {
		return "", fmt.Errorf("для проверки связи не задан ключ рабочей точки")
	}
	shift = application.NormalizeShiftSnapshot(shift)
	if err := application.ValidateShiftSnapshot(shift); err != nil {
		return "", fmt.Errorf("проверить состояние смены EAVC: %w", err)
	}
	probeID, err := protocol.UUIDv7(time.Now().UTC(), rand.Reader)
	if err != nil {
		return "", err
	}
	sentAt := time.Now().UTC().Truncate(time.Second)
	probe := probePayload{
		Schema: probeSchema, ClientID: clientID, ProbeID: probeID,
		SentAt: sentAt.Format(time.RFC3339), Shift: shift,
	}
	probe.MAC = probeMAC(transport.Key, probe.ClientID, probe.ProbeID, probe.SentAt, probe.Shift)
	payload, err := json.Marshal(probe)
	if err != nil {
		return "", err
	}
	_, err = transport.API.SendDocument(
		ctx, transport.ChatID, transport.ThreadID, probeID+".icprobe", payload, "Проверка связи EAVC Connect",
	)
	if err != nil {
		return "", err
	}
	return probeID, nil
}

func probeMAC(key []byte, clientID, probeID, sentAt string, shift application.ShiftSnapshot) string {
	digest := hmac.New(sha256.New, key)
	shiftJSON, _ := json.Marshal(application.NormalizeShiftSnapshot(shift))
	_, _ = digest.Write([]byte(probeSchema + "\x00" + clientID + "\x00" + probeID + "\x00" + sentAt + "\x00"))
	_, _ = digest.Write(shiftJSON)
	return hex.EncodeToString(digest.Sum(nil))
}

// VerifyProbe требует одновременно действующий Telegram-транспорт и ключ точки.
// Ограничение времени не позволяет повторно использовать старый подписанный файл.
func VerifyProbe(packet application.IncomingPacket, key []byte) bool {
	if len(key) != 32 || packet.ProbeClientID == "" || packet.ProbeID == "" ||
		packet.ProbeMAC == "" || packet.ProbeSentAt.IsZero() || packet.SentAt.IsZero() {
		return false
	}
	if err := application.ValidateShiftSnapshot(packet.ProbeShift); err != nil {
		return false
	}
	expected := probeMAC(key, packet.ProbeClientID, packet.ProbeID, packet.ProbeSentAt.Format(time.RFC3339), packet.ProbeShift)
	actual, err := hex.DecodeString(packet.ProbeMAC)
	if err != nil || !hmac.Equal([]byte(expected), []byte(hex.EncodeToString(actual))) {
		return false
	}
	delta := packet.SentAt.Sub(packet.ProbeSentAt)
	return delta >= -time.Minute && delta <= 2*time.Minute
}

// Send загружает один зашифрованный пакет в тему зарегистрированной точки.
func (transport *ClientTransport) Send(ctx context.Context, envelope protocol.Envelope) (int64, error) {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return 0, fmt.Errorf("кодировать транспортный пакет: %w", err)
	}
	caption := fmt.Sprintf("IC/1 %s %s", envelope.ClientID, envelope.EventID)
	message, err := transport.API.SendDocument(
		ctx, transport.ChatID, transport.ThreadID, envelope.EventID+".icevent", payload, caption,
	)
	if err != nil {
		return 0, err
	}
	return message.MessageID, nil
}

// Acknowledgements принимает только машинные ответы указанного серверного бота.
func (transport *ClientTransport) Acknowledgements(ctx context.Context, offset int64) ([]application.Acknowledgement, int64, error) {
	updates, err := transport.API.GetUpdates(ctx, offset, 20)
	if err != nil {
		return nil, offset, err
	}
	next := offset
	var acknowledgements []application.Acknowledgement
	expected := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(transport.ServerBotUsername)), "@")
	for _, update := range updates {
		if update.UpdateID >= next {
			next = update.UpdateID + 1
		}
		message := update.Message
		if message == nil || message.From == nil || !message.From.IsBot {
			continue
		}
		if transport.ServerBotID != 0 && message.From.ID != transport.ServerBotID {
			continue
		}
		if strings.ToLower(message.From.Username) != expected {
			continue
		}
		status := "accepted"
		eventID := ""
		transportMessageID := int64(0)
		switch {
		case strings.HasPrefix(message.Text, probeAcknowledgementPrefix):
			parts := strings.Fields(strings.TrimPrefix(message.Text, probeAcknowledgementPrefix))
			if len(parts) != 2 || !validAcknowledgementMAC(
				transport.Key, probeAcknowledgementSchema, parts[0], "probe", parts[1],
			) {
				continue
			}
			eventID, status = parts[0], "probe"
		case strings.HasPrefix(message.Text, acknowledgementPrefixV4):
			parts := strings.Fields(strings.TrimPrefix(message.Text, acknowledgementPrefixV4))
			if len(parts) != 4 || (parts[1] != "accepted" && parts[1] != "expired") {
				continue
			}
			messageID, parseErr := strconv.ParseInt(parts[2], 10, 64)
			if parseErr != nil || messageID <= 0 || strconv.FormatInt(messageID, 10) != parts[2] ||
				!validAcknowledgementMAC(transport.Key, acknowledgementSchemaV4,
					parts[0], parts[1]+"\x00"+parts[2], parts[3]) {
				continue
			}
			eventID, status, transportMessageID = parts[0], parts[1], messageID
		case strings.HasPrefix(message.Text, acknowledgementPrefixV3):
			parts := strings.Fields(strings.TrimPrefix(message.Text, acknowledgementPrefixV3))
			if len(parts) != 3 || (parts[1] != "accepted" && parts[1] != "expired") ||
				!validAcknowledgementMAC(transport.Key, acknowledgementSchema, parts[0], parts[1], parts[2]) {
				continue
			}
			eventID, status = parts[0], parts[1]
		default:
			continue
		}
		if eventID == "" || strings.ContainsAny(eventID, " \r\n\t") {
			continue
		}
		acknowledgements = append(acknowledgements, application.Acknowledgement{
			EventID: eventID, AcceptedAt: time.Unix(message.Date, 0).UTC(), Status: status,
			TransportMessageID: transportMessageID,
		})
	}
	return acknowledgements, next, nil
}

// AcknowledgementText возвращает единый машинно-читаемый ответ сервера.
func acknowledgementMAC(key []byte, schema, eventID, status string) string {
	digest := hmac.New(sha256.New, key)
	_, _ = digest.Write([]byte(schema + "\x00" + eventID + "\x00" + status))
	return hex.EncodeToString(digest.Sum(nil))
}

func validAcknowledgementMAC(key []byte, schema, eventID, status, value string) bool {
	if len(key) != 32 || strings.TrimSpace(eventID) == "" {
		return false
	}
	actual, err := hex.DecodeString(value)
	if err != nil {
		return false
	}
	expected, _ := hex.DecodeString(acknowledgementMAC(key, schema, eventID, status))
	return hmac.Equal(actual, expected)

}

// AcknowledgementText возвращает подписанный ответ сервера.
func AcknowledgementText(eventID, status string, key []byte) string {
	return acknowledgementPrefixV3 + eventID + " " + status + " " +
		acknowledgementMAC(key, acknowledgementSchema, eventID, status)
}

// AcknowledgementTraceText связывает подписанное подтверждение с тем
// транспортным сообщением, которое Центр действительно обработал.
func AcknowledgementTraceText(eventID, status string, messageID int64, key []byte) string {
	id := strconv.FormatInt(messageID, 10)
	return acknowledgementPrefixV4 + eventID + " " + status + " " + id + " " +
		acknowledgementMAC(key, acknowledgementSchemaV4, eventID, status+"\x00"+id)
}

func probeAcknowledgementText(probeID string, key []byte) string {
	return probeAcknowledgementPrefix + probeID + " " +
		acknowledgementMAC(key, probeAcknowledgementSchema, probeID, "probe")
}
