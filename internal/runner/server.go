package runner

import (
	"context"
	"crypto/ed25519"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"intercept/internal/config"
	"intercept/internal/defaults"
	"intercept/internal/domain"
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
}

// TransportSetup содержит поля первого экрана настройки транспорта.
type TransportSetup struct {
	BotToken   string `json:"bot_token"`
	ChatID     int64  `json:"chat_id"`
	InviteLink string `json:"invite_link"`
}

// DestinationSetup содержит проверяемые поля нового внешнего направления.
type DestinationSetup struct {
	ID       string                 `json:"id"`
	Name     string                 `json:"name"`
	Kind     domain.DestinationKind `json:"kind"`
	BotToken string                 `json:"bot_token,omitempty"`
	ChatID   int64                  `json:"chat_id,omitempty"`
	Template string                 `json:"template,omitempty"`
	Folder   string                 `json:"folder,omitempty"`
	Network  string                 `json:"network,omitempty"`
	Prefix   string                 `json:"prefix,omitempty"`
	Username string                 `json:"username,omitempty"`
	Password string                 `json:"password,omitempty"`
}

// Server владеет жизненным циклом центральной программы. Прикладные правила
// находятся в application/domain, а здесь остаются только сборка и координация.
// Server координирует жизненный цикл, не реализуя правила предметной области.
type Server struct {
	BaseDir string
	Version string
	Logger  *slog.Logger
	Store   *stateDB.ServerStore

	mu          sync.RWMutex
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
			server.fail("Ошибка настроек сервера", err)
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

func (server *Server) applicationContext() context.Context {
	server.mu.RLock()
	defer server.mu.RUnlock()
	if server.rootContext != nil {
		return server.rootContext
	}
	return context.Background()
}
