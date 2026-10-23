package runner

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"intercept/internal/config"
	"intercept/internal/defaults"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/secrets"
	stateDB "intercept/internal/infrastructure/sqlite"
)

const serverOffsetName = "telegram_offset"

const (
	serverTerminalRetention = 30 * 24 * time.Hour
	serverRejectedRetention = 90 * 24 * time.Hour
	serverRuleHistoryLimit  = 100
	serverRejectedLimit     = 1000
)

// ServerStatus содержит только сведения, которые разрешено показывать в интерфейсе.
type ServerStatus struct {
	State        string              `json:"state"`
	Title        string              `json:"title"`
	Detail       string              `json:"detail"`
	Configured   bool                `json:"configured"`
	TransportOK  bool                `json:"transport_ok"`
	LastReceived *time.Time          `json:"last_received,omitempty"`
	LastDelivery *time.Time          `json:"last_delivery,omitempty"`
	Stats        stateDB.ServerStats `json:"stats"`
	Version      string              `json:"version"`
	InviteLink   string              `json:"invite_link,omitempty"`
	Destinations []string            `json:"destinations"`
	Processing   bool                `json:"processing"`
}

// StartProcessing запускает приём и доставку вручную по сохранённым настройкам.
func (server *Server) StartProcessing() error {
	if _, err := os.Stat(server.configPath()); err != nil {
		return errors.New("сначала настройте связь с рабочими точками")
	}
	return server.restartWorker(server.applicationContext())
}

// StopProcessing безопасно останавливает приём и доставку, сохраняя очереди.
func (server *Server) StopProcessing() {
	server.stopWorker()
	server.update(func(status *ServerStatus) {
		status.State = "paused"
		status.Title = "Обработка остановлена"
		status.Detail = "Настройки и очереди сохранены. Для продолжения нажмите «Старт»."
		status.Processing = false
		status.TransportOK = false
	})
}

// RefreshProcessing перечитывает настройки и немедленно переподключает транспорт.
func (server *Server) RefreshProcessing() error {
	return server.StartProcessing()
}

// TransportSetup содержит поля первого экрана настройки транспорта.
type TransportSetup struct {
	BotToken   string `json:"bot_token"`
	ChatID     int64  `json:"chat_id"`
	InviteLink string `json:"invite_link"`
}

// DestinationSetup содержит проверяемые поля нового внешнего направления.
type DestinationSetup struct {
	ID              string                 `json:"id"`
	Name            string                 `json:"name"`
	Kind            domain.DestinationKind `json:"kind"`
	BotToken        string                 `json:"bot_token,omitempty"`
	ChatID          int64                  `json:"chat_id,omitempty"`
	MessageTemplate string                 `json:"message_template,omitempty"`
	Template        string                 `json:"template,omitempty"`
	Folder          string                 `json:"folder,omitempty"`
	Network         string                 `json:"network,omitempty"`
	Protection      string                 `json:"protection,omitempty"`
	Prefix          string                 `json:"prefix,omitempty"`
	Username        string                 `json:"username,omitempty"`
	Password        string                 `json:"password,omitempty"`
}

// TemplateDefaults описывает стандартные шаблоны и обязательные метки.
type TemplateDefaults struct {
	Telegram        string   `json:"telegram"`
	TelegramMarkers []string `json:"telegram_markers"`
	WordPath        string   `json:"word_path"`
	WordMarkers     []string `json:"word_markers"`
}

// Server владеет жизненным циклом центральной программы. Прикладные правила
// находятся в application/domain, а здесь остаются только сборка и координация.
// Server координирует жизненный цикл, не реализуя правила предметной области.
type Server struct {
	BaseDir         string
	Version         string
	UpdatePublicKey string
	Logger          *slog.Logger
	Store           *stateDB.ServerStore

	mu          sync.RWMutex
	adminMu     sync.Mutex
	lifecycleMu sync.Mutex
	status      ServerStatus
	config      config.Server
	workerStop  context.CancelFunc
	workerDone  chan struct{}
	rootContext context.Context
	rootStop    context.CancelFunc
	background  sync.WaitGroup
	signingKey  ed25519.PrivateKey
}

// NewServer открывает состояние и создаёт начальные правила при первом запуске.
func NewServer(ctx context.Context, baseDir, version string, logger *slog.Logger) (*Server, error) {
	db, err := stateDB.OpenState(filepath.Join(baseDir, "data", "server.db"))
	if err != nil {
		return nil, err
	}
	store, err := stateDB.NewServerStore(ctx, db)
	if err != nil {
		db.Close()
		return nil, err
	}
	activeRules, rulesErr := store.ActiveRules(ctx)
	if rulesErr != nil {
		rules, defaultErr := defaults.InitialRules()
		if defaultErr != nil {
			db.Close()
			return nil, defaultErr
		}
		if err := store.PublishRules(ctx, rules, "Начальная конфигурация"); err != nil {
			db.Close()
			return nil, err
		}
	} else if _, configErr := os.Stat(filepath.Join(baseDir, "data", "server-config.dat")); os.IsNotExist(configErr) && isLegacyBuiltInRules(activeRules) {
		rules, defaultErr := defaults.InitialRules()
		if defaultErr != nil {
			db.Close()
			return nil, defaultErr
		}
		rules.Version = activeRules.Version + 1
		if err := store.PublishRules(ctx, rules, "Удалена устаревшая встроенная заготовка"); err != nil {
			db.Close()
			return nil, err
		}
	}
	server := &Server{
		BaseDir: baseDir, Version: version, Logger: logger, Store: store,
		status: ServerStatus{
			State: "setup", Title: "Настройте связь",
			Detail:  "Укажите бота центра и группу, через которую рабочие точки будут обмениваться данными.",
			Version: version,
		},
	}
	return server, nil
}

func isLegacyBuiltInRules(rules domain.RuleSet) bool {
	if len(rules.Destinations) != 3 || len(rules.Positions) < 20 {
		return false
	}
	_, aerodrom := rules.Destinations["aerodrom"]
	_, shabashka := rules.Destinations["shabashka"]
	_, word := rules.Destinations["word-main"]
	return aerodrom && shabashka && word
}

// Start запускает приём, доставку и обслуживание до отмены контекста.
func (server *Server) Start(ctx context.Context) {
	rootContext, rootStop := context.WithCancel(ctx)
	server.mu.Lock()
	server.rootContext = rootContext
	server.rootStop = rootStop
	server.mu.Unlock()
	if _, err := os.Stat(server.configPath()); err == nil {
		if err := server.restartWorker(rootContext); err != nil {
			server.fail(
				"Не удалось загрузить настройки центра",
				"Проверьте сохранённые данные на странице «Связь».",
				err,
			)
		}
	}
	server.background.Add(2)
	go func() {
		defer server.background.Done()
		server.statusLoop(rootContext)
	}()
	go func() {
		defer server.background.Done()
		server.serverMaintenanceLoop(rootContext)
	}()
}

// ValidateForUpdate открывает и мигрирует внутреннее хранилище (это уже
// выполнено NewServer), затем проверяет защищённую конфигурацию без запуска
// Telegram, Word-доставок и фонового обслуживания.
func (server *Server) ValidateForUpdate() (resultErr error) {
	defer func() {
		if resultErr != nil {
			server.fail("Проверка обновления не пройдена", "Предыдущая версия будет восстановлена автоматически.", resultErr)
		}
	}()
	protected, err := secrets.Load(server.configPath())
	if errors.Is(err, os.ErrNotExist) {
		server.update(func(status *ServerStatus) {
			status.State = "paused"
			status.Title = "Обновление проверено"
			status.Detail = "Центр ещё не настроен; новая версия запускается."
			status.Processing = false
		})
		return nil
	}
	if err != nil {
		return err
	}
	cfg, err := config.DecodeServer(bytes.NewReader(protected))
	if err != nil {
		return err
	}
	server.mu.Lock()
	server.config = cloneServerConfig(cfg)
	server.status.State = "paused"
	server.status.Title = "Обновление проверено"
	server.status.Detail = "Настройки и рабочие данные совместимы. Рабочие циклы ещё не запускались."
	server.status.Configured = true
	server.status.InviteLink = cfg.Transport.InviteLink
	server.status.Destinations = destinationIDs(cfg.Destinations)
	server.status.Processing = false
	server.mu.Unlock()
	return nil
}

// Close штатно останавливает фоновые процессы и закрывает хранилище.
func (server *Server) Close() {
	server.mu.Lock()
	rootStop := server.rootStop
	server.rootStop = nil
	server.mu.Unlock()
	if rootStop != nil {
		rootStop()
	}
	server.stopWorker()
	server.background.Wait()
	_ = server.Store.Close()
}

// Status возвращает безопасный снимок состояния для экрана администратора.
func (server *Server) Status() ServerStatus {
	server.mu.RLock()
	defer server.mu.RUnlock()
	return server.status
}

// OperationalStatus contains only technical health data suitable for a
// diagnostic archive. The private Telegram invitation is UI-only.
func (server *Server) OperationalStatus() ServerStatus {
	status := server.Status()
	status.InviteLink = ""
	return status
}

func (server *Server) applicationContext() context.Context {
	server.mu.RLock()
	defer server.mu.RUnlock()
	if server.rootContext != nil {
		return server.rootContext
	}
	return context.Background()
}
