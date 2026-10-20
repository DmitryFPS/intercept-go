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

	"intercept/internal/infrastructure/secrets"
	"intercept/internal/interfaces/clientui"
	"intercept/internal/interfaces/localui"
	"intercept/internal/observability"
	"intercept/internal/runner"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Printf("EAVC Connect Station %s\n", buildVersion())
		return
	}
	baseDir, err := executableDir()
	if err != nil {
		panic(err)
	}
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
	openBrowser := len(os.Args) < 2 || os.Args[1] != "--no-browser"
	if openBrowser && localui.IsRunning(address, accessToken) {
		_ = localui.OpenBrowser(localui.AccessURL(address, accessToken))
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	application := runner.NewClient(baseDir, buildVersion(), logger)
	application.Start(ctx)
	handler := clientui.Handler{Client: application, Context: ctx}.Routes()
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
