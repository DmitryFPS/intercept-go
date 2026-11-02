package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/sqlite"
	"intercept/internal/infrastructure/telegram"
	"intercept/internal/protocol"
)

func TestAcknowledgementTrackerSerializesSameAcknowledgement(t *testing.T) {
	t.Parallel()
	tracker := newAcknowledgementTracker(nil)
	now := time.Now().UTC()
	const key = "event-1|v4|accepted"

	start := make(chan struct{})
	var successes atomic.Int32
	var workers sync.WaitGroup
	for range 64 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			if tracker.reserve(key, now) {
				successes.Add(1)
			}
		}()
	}
	close(start)
	workers.Wait()
	if got := successes.Load(); got != 1 {
		t.Fatalf("одинаковый ACK одновременно зарезервирован %d раз вместо одного", got)
	}
	if tracker.reserve(key, now.Add(time.Second)) {
		t.Fatal("повторный ACK прошёл, пока первая отправка ещё выполняется")
	}

	tracker.release(key)
	if !tracker.reserve(key, now.Add(2*time.Second)) {
		t.Fatal("ошибка отправки не освободила ACK для долговечной попытки")
	}
	completedAt := now.Add(2 * time.Second)
	tracker.complete(key, completedAt)
	if tracker.reserve(key, completedAt.Add(duplicateAcknowledgementInterval-time.Nanosecond)) {
		t.Fatal("успешный ACK повторился внутри защитного окна")
	}
	if !tracker.reserve(key, completedAt.Add(duplicateAcknowledgementInterval)) {
		t.Fatal("ACK не разрешён после полного защитного окна")
	}
	tracker.release(key)
	if !tracker.reserve("event-2|v4|accepted", completedAt) {
		t.Fatal("ACK другого события ошибочно заблокирован")
	}
}

// Медленное удаление файла из транспортной группы и медленный ACK одной точки
// не должны занимать worker двух других точек. Тест удерживает оба HTTP-запроса
// открытыми и проверяет, что оставшиеся полосы успевают подтвердить события.
func TestTransportRuntimeCleanupDoesNotBlockThreeSenderLanes(t *testing.T) {
	ctx := context.Background()
	store := newRunnerTransportStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	packets := persistParallelRuntimePackets(t, ctx, store, now, 3)
	if err := store.PersistTransportBatch(ctx, serverOffsetName, 4, packets); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueTransportDeletion(ctx, -1001, 999); err != nil {
		t.Fatal(err)
	}

	deleteStarted := make(chan struct{})
	deleteRelease := make(chan struct{})
	firstACKStarted := make(chan struct{})
	firstACKRelease := make(chan struct{})
	acknowledged := make(chan string, 6)
	var deleteOnce, firstACKOnce sync.Once
	apiServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/deleteMessage"):
			deleteOnce.Do(func() { close(deleteStarted) })
			select {
			case <-deleteRelease:
			case <-request.Context().Done():
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		case strings.HasSuffix(request.URL.Path, "/sendMessage"):
			var input struct {
				ChatID string `json:"chat_id"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
			if input.ChatID == "@station_1_bot" {
				firstACKOnce.Do(func() { close(firstACKStarted) })
				select {
				case <-firstACKRelease:
				case <-request.Context().Done():
					return
				}
			}
			acknowledged <- input.ChatID
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true,
				"result": map[string]any{
					"message_id": 500, "date": now.Unix(),
					"chat": map[string]any{"id": 1, "type": "private"},
				},
			})
		default:
			http.Error(writer, "unexpected request", http.StatusNotFound)
		}
	}))
	defer apiServer.Close()
	var releaseDeleteOnce, releaseFirstACKOnce sync.Once
	defer releaseDeleteOnce.Do(func() { close(deleteRelease) })
	defer releaseFirstACKOnce.Do(func() { close(firstACKRelease) })

	api, err := telegram.NewWithBaseURL("123:token", apiServer.URL, apiServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := &telegram.ServerReceiver{
		API: api, ChatID: -1001, MaxBytes: 1 << 20,
		Observe: func(application.TransportObservation) {},
	}
	server := &Server{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	reception := &application.ServerService{
		Clients: store, Rules: store, Store: store,
		Now: func() time.Time { return now }, DeliveryWindow: time.Hour,
	}
	loopContext, cancel := context.WithCancel(context.Background())
	var loops sync.WaitGroup
	loops.Add(2)
	go func() {
		defer loops.Done()
		server.transportCleanupLoop(loopContext, receiver)
	}()
	select {
	case <-deleteStarted:
	case <-time.After(2 * time.Second):
		cancel()
		loops.Wait()
		t.Fatal("фоновое удаление не началось")
	}
	go func() {
		defer loops.Done()
		server.transportInboxLoop(loopContext, receiver, reception)
	}()
	defer func() {
		cancel()
		releaseDeleteOnce.Do(func() { close(deleteRelease) })
		releaseFirstACKOnce.Do(func() { close(firstACKRelease) })
		loops.Wait()
	}()

	select {
	case <-firstACKStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("первая полоса не дошла до удерживаемого ACK")
	}
	want := map[string]bool{"@station_2_bot": false, "@station_3_bot": false}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for !want["@station_2_bot"] || !want["@station_3_bot"] {
		select {
		case bot := <-acknowledged:
			if _, exists := want[bot]; exists {
				want[bot] = true
			}
		case <-deadline.C:
			t.Fatalf("независимые полосы задержаны: подтверждения=%+v", want)
		}
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Events != 3 {
		t.Fatalf("пока cleanup и первый ACK заблокированы, принято событий %d вместо 3", stats.Events)
	}

	releaseFirstACKOnce.Do(func() { close(firstACKRelease) })
	if !waitForParallelRuntime(t, 2*time.Second, func() bool {
		stats, statsErr := store.Stats(context.Background())
		return statsErr == nil && stats.TransportInbox == 0 && stats.Events == 3
	}) {
		stats, _ = store.Stats(context.Background())
		t.Fatalf("входные полосы не завершились после освобождения ACK: %+v", stats)
	}
	select {
	case <-deleteRelease:
		t.Fatal("тест самопроизвольно освободил удерживаемое удаление")
	default:
	}
}

func TestTransportInboxWorkerCancellationReleasesLeaseImmediately(t *testing.T) {
	ctx := context.Background()
	store := newRunnerTransportStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	packets := persistParallelRuntimePackets(t, ctx, store, now, 1)
	if err := store.PersistTransportBatch(ctx, serverOffsetName, 2, packets); err != nil {
		t.Fatal(err)
	}

	ackStarted := make(chan struct{})
	ackHandlerRelease := make(chan struct{})
	var ackOnce sync.Once
	apiServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/sendMessage") {
			http.Error(writer, "unexpected request", http.StatusNotFound)
			return
		}
		ackOnce.Do(func() { close(ackStarted) })
		select {
		case <-request.Context().Done():
		case <-ackHandlerRelease:
		}
	}))
	defer apiServer.Close()
	defer close(ackHandlerRelease)
	api, err := telegram.NewWithBaseURL("123:token", apiServer.URL, apiServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := &telegram.ServerReceiver{
		API: api, ChatID: -1001, MaxBytes: 1 << 20,
		Observe: func(application.TransportObservation) {},
	}
	server := &Server{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	reception := &application.ServerService{
		Clients: store, Rules: store, Store: store,
		Now: func() time.Time { return now }, DeliveryWindow: time.Hour,
	}
	workerContext, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.transportInboxWorker(workerContext, receiver, reception, newAcknowledgementTracker(nil))
	}()
	select {
	case <-ackStarted:
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("worker не дошёл до удерживаемого ACK")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker не остановился после отмены контекста")
	}

	claimed, err := store.ClaimTransportPackets(context.Background(), 1, time.Now().UTC(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].Packet.UpdateID != packets[0].UpdateID {
		t.Fatalf("остановленный worker не освободил lease немедленно: %#v", claimed)
	}
	if claimed[0].Attempts != 0 {
		t.Fatalf("штатная остановка ошибочно увеличила attempts до %d", claimed[0].Attempts)
	}
}

func persistParallelRuntimePackets(
	t *testing.T, ctx context.Context, store *sqlite.ServerStore, now time.Time, count int,
) []application.IncomingPacket {
	t.Helper()
	rules := domain.RuleSet{
		Version: 1,
		Destinations: map[string]domain.Destination{
			"telegram": {ID: "telegram", Name: "Получатель", Kind: domain.DestinationTelegram, Enabled: true},
		},
		Positions: map[string]map[string]domain.RouteDecision{
			"name:" + domain.NormalizeUnitName("подразделение"): {
				"telegram": {Action: domain.RouteDeliver, ThreadID: 7},
			},
		},
		Unknown: map[string]domain.RouteDecision{"telegram": {Action: domain.RouteReview}},
	}
	if err := store.PublishRules(ctx, rules, "parallel-runtime-test"); err != nil {
		t.Fatal(err)
	}
	packets := make([]application.IncomingPacket, 0, count)
	for index := 1; index <= count; index++ {
		key := bytes.Repeat([]byte{byte(0x30 + index)}, 32)
		client := application.RegisteredClient{
			ID: fmt.Sprintf("station-%d", index), Number: fmt.Sprintf("%03d", index),
			Name: fmt.Sprintf("Точка %d", index), SenderBotID: int64(100 + index),
			SenderBotUsername: fmt.Sprintf("station_%d_bot", index),
			ChatID:            -1001, ThreadID: int64(10 + index),
			KeyID: fmt.Sprintf("key-%d", index), Key: key, Active: true,
		}
		if err := store.SaveClient(ctx, client); err != nil {
			t.Fatal(err)
		}
		event := domain.Event{
			ID: fmt.Sprintf("event-%d", index), ClientID: client.ID,
			CreatedAt: now, ObservedAt: now, SourceAt: now,
			ExpiresAt: now.Add(30 * time.Minute),
			Source: domain.SourceRecord{
				ID: int64(index), UUID: fmt.Sprintf("source-%d", index),
			},
			Position: domain.Position{UnitName: "подразделение"},
			Mode:     domain.SourceModeNormal, Content: fmt.Sprintf("Осмысленный перехват точки %d", index),
			ContentDigestVersion: domain.ContentDigestVersionCurrent,
		}
		event.ContentDigest = event.CanonicalContentDigest()
		envelope, err := protocol.Seal(event, client.KeyID, key, now,
			bytes.NewReader(bytes.Repeat([]byte{byte(index)}, 12)))
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		packets = append(packets, application.IncomingPacket{
			UpdateID: int64(index), SenderBotID: client.SenderBotID,
			SenderBotUsername: client.SenderBotUsername,
			ChatID:            client.ChatID, ThreadID: client.ThreadID,
			MessageID: int64(1000 + index), SentAt: now, ReceivedAt: now,
			Payload: payload,
		})
	}
	return packets
}

func waitForParallelRuntime(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return condition()
}
