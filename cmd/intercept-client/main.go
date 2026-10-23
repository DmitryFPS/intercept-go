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
	"intercept/internal/interfaces/clientui"
	"intercept/internal/interfaces/localui"
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
		fmt.Printf("EAVC Connect Station %s\n", buildVersion())
		return
	}
	baseDir, err := executableDir()
	if err != nil {
		panic(err)
	}
	executable, _ := os.Executable()
	logger, closer, err := observability.NewLogger(filepath.Join(baseDir, "logs", "client.log"), "client")
	if err != nil {
		logger = slog.Default()
	} else {
		defer closer.Close()
	}
	accessToken, err := secrets.LoadOrCreateToken(
		filepath.Join(baseDir, "data", "client-ui-access.dat"), argumentValue("--access-token="),
	)
	if err != nil {
		logger.Error("не удалось защитить локальный интерфейс", "error", err)
		return
	}
	address := "127.0.0.1:17631"
	if err := autostart.Ensure("EAVC Connect Station", executable); err != nil {
		logger.Warn("не удалось включить автозапуск", "error", err)
	}
	openBrowser := len(os.Args) < 2 || os.Args[1] != "--no-browser"
	releaseInstance, alreadyRunning, err := singleinstance.Acquire("EAVC-Connect-Station")
	if err != nil {
		logger.Error("не удалось проверить запущенный экземпляр", "error", err)
		return
	}
	defer releaseInstance()
	if alreadyRunning {
		if openBrowser {
			for attempt := 0; attempt < 20 && !localui.IsRunning(address, accessToken); attempt++ {
				time.Sleep(100 * time.Millisecond)
			}
			_ = localui.OpenBrowser(localui.AccessURL(address, accessToken))
		}
		return
	}
	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, shutdown := context.WithCancel(signalContext)
	defer shutdown()
	application := runner.NewClient(baseDir, buildVersion(), logger)
	defer application.Close()
	application.Start(ctx)
	observability.StartOperationalRecorder(ctx, baseDir, "station", buildVersion(), func() any { return application.Status() })
	updater := selfupdate.NewManager(selfupdate.Configuration{
		BaseDir: baseDir, Product: "eavc-connect-station", Version: buildVersion(),
		PublicKey: updatePublicKey, Application: filepath.Base(executable), Helper: "EAVC Connect Updater.exe",
		HealthAddress: address, TokenFile: filepath.Join("data", "client-ui-access.dat"), Logger: logger,
	})
	handler := clientui.Handler{Client: application, Context: ctx, Updater: updater, Shutdown: shutdown}.Routes()
	if err := localui.ListenAndServe(ctx, address, handler, openBrowser, accessToken); err != nil {
		logger.Error("локальный интерфейс завершён", "error", err)
		_ = localui.OpenBrowser(localui.AccessURL(address, accessToken))
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
