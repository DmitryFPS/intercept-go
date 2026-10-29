package runner

import (
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/domain"
)

// Черновик настроек можно менять в панели, не затрагивая уже работающую копию сервера.
func TestCloneServerConfigDoesNotShareDestinationMap(t *testing.T) {
	t.Parallel()
	original := config.Server{Destinations: map[string]config.DestinationConfig{
		"a": {Kind: domain.DestinationTelegram, ChatID: 1},
	}}
	clone := cloneServerConfig(original)
	clone.Destinations["b"] = config.DestinationConfig{Kind: domain.DestinationWord}
	if _, exists := original.Destinations["b"]; exists {
		t.Fatal("копия конфигурации разделяет map направлений")
	}
}

func TestRecentTransportPacketRejectsBacklogAndFutureTime(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	if !recentTransportPacket(now.Add(-time.Minute), now) {
		t.Fatal("свежий пакет отклонён")
	}
	if recentTransportPacket(now.Add(-6*time.Minute), now) {
		t.Fatal("старый пакет очереди принят за текущую связь")
	}
	if recentTransportPacket(now.Add(2*time.Minute), now) {
		t.Fatal("пакет с будущим временем принят за текущую связь")
	}
}

func TestDuplicateAcknowledgementIsSuppressedOnlyTemporarily(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	recent := map[string]time.Time{"event-1": now}
	if acknowledgementDue(recent, "event-1", now.Add(time.Minute)) {
		t.Fatal("плотный повтор одного подтверждения не подавлен")
	}
	if !acknowledgementDue(recent, "event-1", now.Add(duplicateAcknowledgementInterval)) {
		t.Fatal("повтор после защитного интервала должен быть разрешён")
	}
	if !acknowledgementDue(recent, "event-2", now) {
		t.Fatal("подтверждение другого события ошибочно подавлено")
	}
}

func TestAcknowledgementOutcomeChangeBypassesDuplicateWindow(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	recent := make(map[string]time.Time)
	for _, traceV4 := range []bool{false, true} {
		expired := application.PacketResult{EventID: "event-1", Status: "expired", AckTraceV4: traceV4}
		accepted := application.PacketResult{EventID: "event-1", Status: "accepted", AckTraceV4: traceV4}
		expiredKey, acceptedKey := acknowledgementKey(expired), acknowledgementKey(accepted)
		if !acknowledgementDue(recent, expiredKey, now) {
			t.Fatal("первый expired ACK подавлен")
		}
		recent[expiredKey] = now
		if !acknowledgementDue(recent, acceptedKey, now.Add(time.Second)) {
			t.Fatalf("accepted ACK подавлен после expired: v4=%v", traceV4)
		}
		recent[acceptedKey] = now.Add(time.Second)
		if acknowledgementDue(recent, acceptedKey, now.Add(2*time.Second)) {
			t.Fatalf("дубль accepted ACK не подавлен: v4=%v", traceV4)
		}
	}
}

func TestTransportInboxRetryDelayIsBounded(t *testing.T) {
	t.Parallel()
	if got := transportInboxRetryDelay(0); got != 5*time.Second {
		t.Fatalf("первая задержка: %s", got)
	}
	if got := transportInboxRetryDelay(100); got != 10*time.Minute {
		t.Fatalf("верхняя граница повтора: %s", got)
	}
}

func TestClearDestinationThreads(t *testing.T) {
	t.Parallel()
	rules := domain.RuleSet{
		Unknown: map[string]domain.RouteDecision{"receiver": {Action: domain.RouteDeliver, ThreadID: 11}},
		Positions: map[string]map[string]domain.RouteDecision{
			"name:test": {"receiver": {Action: domain.RouteReview, ThreadID: 22}},
		},
	}
	clearDestinationThreads(rules, "receiver")
	if rules.Unknown["receiver"].ThreadID != 0 ||
		rules.Positions["name:test"]["receiver"].ThreadID != 0 {
		t.Fatal("ID темы остался после переключения получателя с Telegram")
	}
}

func TestLegacyBuiltInRulesDetection(t *testing.T) {
	t.Parallel()
	legacy := domain.RuleSet{Destinations: map[string]domain.Destination{
		"aerodrom": {}, "shabashka": {}, "word-main": {},
	}, Positions: map[string]map[string]domain.RouteDecision{}}
	for index := 0; index < 20; index++ {
		legacy.Positions[string(rune('a'+index))] = nil
	}
	if !isLegacyBuiltInRules(legacy) {
		t.Fatal("старая встроенная заготовка не распознана")
	}
	delete(legacy.Destinations, "word-main")
	if isLegacyBuiltInRules(legacy) {
		t.Fatal("пользовательские правила ошибочно распознаны как старая заготовка")
	}
}

func TestDestinationStateScopesFollowExternalIdentity(t *testing.T) {
	t.Parallel()
	if telegramStateScope(101, "old-token", -1001) != telegramStateScope(101, "rotated-token", -1001) {
		t.Fatal("ротация токена того же бота сбросила историю Telegram и создаст дубль")
	}
	if telegramStateScope(101, "token", -1001) == telegramStateScope(102, "token", -1001) {
		t.Fatal("другой Telegram-бот получил прежнюю область состояния")
	}
	if telegramStateScope(101, "token", -1001) == telegramStateScope(101, "token", -1002) {
		t.Fatal("другая Telegram-группа получила прежнюю область состояния")
	}
	if wordStateScope(`C:\\Shared\\39`) == wordStateScope(`C:\\Shared\\40`) {
		t.Fatal("разные Word-папки получили одинаковую область состояния")
	}
}

func TestDestinationTargetChangedUsesStableBotIdentity(t *testing.T) {
	t.Parallel()
	before := config.DestinationConfig{Kind: domain.DestinationTelegram, BotID: 101, BotToken: "old", ChatID: -1001}
	after := before
	after.BotToken = "rotated"
	if destinationTargetChanged(before, after) {
		t.Fatal("ротация токена того же Telegram-бота принята за смену получателя")
	}
	after.ChatID = -1002
	if !destinationTargetChanged(before, after) {
		t.Fatal("смена Telegram-группы не распознана")
	}
	wordBefore := config.DestinationConfig{Kind: domain.DestinationWord, Folder: `C:\\Shared\\39`}
	wordAfter := wordBefore
	wordAfter.Folder = `C:\\Shared\\40`
	if !destinationTargetChanged(wordBefore, wordAfter) {
		t.Fatal("смена Word-папки не распознана")
	}
}

func TestPublishingStaleRuleVersionIsRejected(t *testing.T) {
	t.Parallel()
	if err := requireCurrentRuleVersion(4, 5); err == nil {
		t.Fatal("устаревшая копия маршрутов может затереть новую")
	}
	if err := requireCurrentRuleVersion(5, 5); err != nil {
		t.Fatalf("текущая версия ошибочно отклонена: %v", err)
	}
}
