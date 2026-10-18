package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"

	"intercept/internal/interfaces/localui"
	"intercept/internal/interfaces/serverui"
	"intercept/internal/observability"
	"intercept/internal/runner"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--version" {
		fmt.Printf("InterceptServer %s\n", buildVersion())
		return
	}
	baseDir, err := executableDir()
	if err != nil {
		panic(err)
	}
	logger, closer, err := observability.NewLogger(filepath.Join(baseDir, "logs", "server.log"), "server")
	if err != nil {
		logger = slog.Default()
	} else {
		defer closer.Close()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	application, err := runner.NewServer(ctx, baseDir, buildVersion(), logger)
	if err != nil {
		logger.Error("не удалось запустить сервер", "error", err)
		return
	}
	defer application.Close()
	application.Start(ctx)
	handler := serverui.Handler{Server: application, Context: ctx}.Routes()
	openBrowser := len(os.Args) < 2 || os.Args[1] != "--no-browser"
	if err := localui.ListenAndServe(ctx, "127.0.0.1:17632", handler, openBrowser); err != nil {
		logger.Error("локальный интерфейс завершён", "error", err)
		_ = localui.OpenBrowser("http://127.0.0.1:17632")
	}
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
