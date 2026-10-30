package serverui

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
	stateDB "intercept/internal/infrastructure/sqlite"
	"intercept/internal/runner"
)

// Проверяем именно локальный HTTP API, которым пользуются все шесть карточек.
// База, события и маршруты существуют только в t.TempDir(): ни одна ВМ и
// действующая очередь не меняются. В каждом состоянии есть реальные строки,
// поэтому отсутствие данных не может быть ошибочно зачтено как успех фильтра.
func TestDashboardHistoryHTTPFiltersWithNonemptyStates(t *testing.T) {
	ctx := context.Background()
	server, err := runner.NewServer(ctx, t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	clients := []application.RegisteredClient{
		{ID: "textile", Number: "01", Name: "Текстиль", SenderBotID: 101, SenderBotUsername: "textile_bot", ChatID: -1001, ThreadID: 10, KeyID: "key-textile", Key: make([]byte, 32), Active: true},
		{ID: "selidovo", Number: "02", Name: "Селидово", SenderBotID: 102, SenderBotUsername: "selidovo_bot", ChatID: -1001, ThreadID: 11, KeyID: "key-selidovo", Key: make([]byte, 32), Active: true},
	}
	for _, client := range clients {
		if err := server.Store.SaveClient(ctx, client); err != nil {
			t.Fatal(err)
		}
	}
	rules := domain.RuleSet{
		Version: 100,
		Destinations: map[string]domain.Destination{
			"a": {ID: "a", Name: "Аэродром", Kind: domain.DestinationTelegram, Enabled: true},
			"w": {ID: "w", Name: "39-ка", Kind: domain.DestinationWord, Enabled: true},
		},
		Positions: map[string]map[string]domain.RouteDecision{
			"name:" + domain.NormalizeUnitName("Игровое подразделение"): {
				"a": {Action: domain.RouteDeliver, ThreadID: 2},
				"w": {Action: domain.RouteDeliver},
			},
		},
		Unknown: map[string]domain.RouteDecision{"a": {Action: domain.RouteReview}},
	}
	if err := server.Store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, seed := range []struct{ id, client string }{
		{"textile-delivery", "textile"}, {"textile-pending", "textile"},
		{"selidovo-delivery", "selidovo"}, {"selidovo-pending", "selidovo"},
	} {
		event := domain.Event{
			ID: seed.id, ClientID: seed.client, CreatedAt: now, ObservedAt: now,
			Source:   domain.SourceRecord{ID: 1, UUID: "source-" + seed.id},
			Position: domain.Position{UnitName: "Игровое подразделение"},
			Mode:     domain.SourceModeNormal, Content: "текст " + seed.id,
			ContentDigest: "digest-" + seed.id,
		}
		plan, err := rules.Resolve(event)
		if err != nil {
			t.Fatal(err)
		}
		if accepted, err := server.Store.Accept(ctx, event, plan); err != nil || !accepted {
			t.Fatalf("accept %s: accepted=%v err=%v", seed.id, accepted, err)
		}
	}
	for _, delivery := range []struct{ event, destination, state string }{
		{"textile-delivery", "a", "delivered"}, {"textile-delivery", "w", "expired"},
		{"selidovo-delivery", "w", "delivered"}, {"selidovo-delivery", "a", "expired"},
	} {
		if delivery.state == "delivered" {
			err = server.Store.MarkDelivered(ctx, delivery.event, delivery.destination, now)
		} else {
			err = server.Store.MarkDeliveryExpired(ctx, delivery.event, delivery.destination, now, "older_than_delivery_window")
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := server.Store.RecordClientSignalByTransport(ctx, 101, -1001, 10, true, now); err != nil {
		t.Fatal(err)
	}
	if err := server.Store.RecordClientSignalByTransport(ctx, 102, -1001, 11, true, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := server.Store.EvaluateClientHealth(ctx, now, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	for _, rejected := range []struct {
		id       int64
		username string
	}{
		{999, "foreign_bot"}, {998, "another_bot"},
	} {
		if err := server.Store.Quarantine(ctx, application.IncomingPacket{
			SenderBotID: rejected.id, SenderBotUsername: rejected.username,
			ChatID: -1001, ThreadID: 77, Payload: []byte(rejected.username),
		}, "unknown_client"); err != nil {
			t.Fatal(err)
		}
	}

	handler := Handler{Server: server}.Routes()
	getPage := func(kind string, filters url.Values) stateDB.ServerHistoryPage {
		t.Helper()
		path := "/api/history/" + kind + "?limit=30&offset=0"
		if len(filters) != 0 {
			path += "&" + filters.Encode()
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s: HTTP %d: %s", path, response.Code, response.Body.String())
		}
		var page stateDB.ServerHistoryPage
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		return page
	}
	keys := func(page stateDB.ServerHistoryPage) []string {
		result := make([]string, 0, len(page.Clients)+len(page.Items))
		for _, client := range page.Clients {
			result = append(result, client.ClientID)
		}
		for _, item := range page.Items {
			if item.State == "rejected" {
				result = append(result, item.ClientName)
				continue
			}
			result = append(result, fmt.Sprintf("%s/%s", item.ID, item.DestinationID))
		}
		sort.Strings(result)
		return result
	}
	for _, test := range []struct {
		name, kind string
		filters    url.Values
		want       []string
	}{
		{"точки: поиск", "clients", url.Values{"q": {"Текстиль"}}, []string{"textile"}},
		{"точки: ID", "clients", url.Values{"client": {"textile"}}, []string{"textile"}},
		{"точки: качество", "clients", url.Values{"quality": {"good"}}, []string{"textile"}},
		{"получено: поиск", "received", url.Values{"q": {"textile-delivery"}}, []string{"textile-delivery/"}},
		{"получено: точка", "received", url.Values{"client": {"textile"}}, []string{"textile-delivery/", "textile-pending/"}},
		{"ожидает: поиск", "pending", url.Values{"q": {"textile-pending"}}, []string{"textile-pending/a", "textile-pending/w"}},
		{"ожидает: точка", "pending", url.Values{"client": {"textile"}}, []string{"textile-pending/a", "textile-pending/w"}},
		{"ожидает: получатель", "pending", url.Values{"destination": {"w"}}, []string{"selidovo-pending/w", "textile-pending/w"}},
		{"доставлено: поиск", "delivered", url.Values{"q": {"textile-delivery"}}, []string{"textile-delivery/a"}},
		{"доставлено: точка", "delivered", url.Values{"client": {"textile"}}, []string{"textile-delivery/a"}},
		{"доставлено: получатель", "delivered", url.Values{"destination": {"a"}}, []string{"textile-delivery/a"}},
		{"просрочено: поиск", "expired", url.Values{"q": {"textile-delivery"}}, []string{"textile-delivery/w"}},
		{"просрочено: точка", "expired", url.Values{"client": {"textile"}}, []string{"textile-delivery/w"}},
		{"просрочено: получатель", "expired", url.Values{"destination": {"w"}}, []string{"textile-delivery/w"}},
		{"отклонено: поиск", "rejected", url.Values{"q": {"foreign_bot"}}, []string{"foreign_bot"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			page := getPage(test.kind, test.filters)
			got := keys(page)
			if len(got) != len(test.want) {
				t.Fatalf("%s: got %v, want %v; total=%d", test.kind, got, test.want, page.Total)
			}
			for index := range got {
				if got[index] != test.want[index] {
					t.Fatalf("%s: got %v, want %v", test.kind, got, test.want)
				}
			}
			if test.kind != "clients" && page.Total != len(test.want) {
				t.Fatalf("%s: total=%d, want=%d", test.kind, page.Total, len(test.want))
			}
		})
	}
}
