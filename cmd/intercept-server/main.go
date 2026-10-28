package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"intercept/internal/autostart"
	"intercept/internal/infrastructure/secrets"
	"intercept/internal/interfaces/localui"
	"intercept/internal/interfaces/serverui"
	"intercept/internal/observability"
	"intercept/internal/runner"
	"intercept/internal/selfupdate"
	"intercept/internal/singleinstance"
)

var (
	version         = "dev"
	updatePublicKey = ""
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Printf("EAVC Connect Center %s\n", buildVersion())
		return
	}
	baseDir, err := executableDir()
	if err != nil {
		panic(err)
	}
	executable, _ := os.Executable()
	logger, closer, err := observability.NewLogger(filepath.Join(baseDir, "logs", "server.log"), "server")
	if err != nil {
		logger = slog.Default()
	} else {
		defer closer.Close()
	}
	accessToken, err := secrets.LoadOrCreateToken(
		filepath.Join(baseDir, "data", "server-ui-access.dat"), argumentValue("--access-token="),
	)
	if err != nil {
		logger.Error("не удалось защитить локальный интерфейс", "error", err)
		return
	}
	address := "127.0.0.1:17632"
	openBrowser := len(os.Args) < 2 || os.Args[1] != "--no-browser"
	releaseInstance, alreadyRunning, err := singleinstance.Acquire("EAVC-Connect-Center")
	if err != nil {
		logger.Error("не удалось проверить запущенный экземпляр", "error", err)
		return
	}
	defer releaseInstance()
	if alreadyRunning {
		if openBrowser {
			verified := false
			for attempt := 0; attempt < 20; attempt++ {
				if verified = localui.IsRunning(address, accessToken); verified {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if verified {
				_ = localui.OpenBrowser(localui.AccessURL(address, accessToken))
			} else {
				logger.Error("занятый порт не принадлежит EAVC Connect; защищённая ссылка не открыта", "address", address)
			}
		}
		return
	}
	// Только экземпляр-владелец обновляет автозапуск. Иначе запущенная из
	// временной папки копия могла заменить рабочий путь в реестре.
	if err := autostart.Ensure("EAVC Connect Center", executable); err != nil {
		logger.Warn("не удалось включить автозапуск", "error", err)
	}
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, shutdown := context.WithCancel(signalContext)
	defer shutdown()
	application, err := runner.NewServer(ctx, baseDir, buildVersion(), logger)
	if err != nil {
		logger.Error("не удалось запустить сервер", "error", err)
		return
	}
	application.UpdatePublicKey = updatePublicKey
	defer application.Close()
	if hasArgument("--update-validation") {
		if err := application.ValidateForUpdate(); err != nil {
			logger.Error("изолированная проверка обновления не пройдена", "error", err)
		}
	} else {
		application.Start(ctx)
	}
	operationalErrors := observability.StartOperationalRecorder(ctx, baseDir, "center", buildVersion(), func() any { return application.OperationalStatus() })
	go func() {
		for err := range operationalErrors {
			logger.Error("не удалось записать эксплуатационный журнал", "error", err)
		}
	}()
	updater := selfupdate.NewManager(selfupdate.Configuration{
		BaseDir: baseDir, Product: "eavc-connect-center", Version: buildVersion(),
		PublicKey: updatePublicKey, Application: filepath.Base(executable), Helper: "EAVC Connect Updater.exe",
		HealthAddress: address, TokenFile: filepath.Join("data", "server-ui-access.dat"), Logger: logger,
	})
	handler := serverui.Handler{Server: application, Context: ctx, Updater: updater, Shutdown: shutdown}.Routes()
	if err := localui.ListenAndServe(ctx, address, handler, openBrowser, accessToken); err != nil {
		logger.Error("локальный интерфейс завершён", "error", err)
	}
}

func argumentValue(prefix string) string {
	for _, argument := range os.Args[1:] {
		if strings.HasPrefix(argument, prefix) {
			return strings.TrimPrefix(argument, prefix)
		}
	}
	return ""
}

func hasArgument(expected string) bool {
	for _, argument := range os.Args[1:] {
		if argument == expected {
			return true
		}
	}
	return false
}

func executableDir() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	return filepath.Dir(executable), nil
}

func buildVersion() string {
	if version != "dev" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return version
}
