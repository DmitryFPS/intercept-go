package clientui

import (
	"context"
	"embed"
	"io"
	"io/fs"
	"net/http"
	"time"

	"intercept/internal/interfaces/localui"
	"intercept/internal/runner"
)

//go:embed assets/*
var assets embed.FS

type Handler struct {
	Client  *runner.Client
	Context context.Context
}

func (handler Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(assets, "assets")
	mux.Handle("GET /", http.FileServer(http.FS(static)))
	mux.HandleFunc("GET /api/status", func(writer http.ResponseWriter, _ *http.Request) {
		localui.WriteJSON(writer, http.StatusOK, handler.Client.Status())
	})
	mux.HandleFunc("POST /api/config/import", handler.importConfig)
	mux.HandleFunc("GET /api/diagnostics", handler.diagnostics)
	return securityHeaders(mux)
}

func (handler Handler) importConfig(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, 2*1024*1024)
	if err := request.ParseMultipartForm(2 * 1024 * 1024); err != nil {
		localui.WriteJSON(writer, http.StatusBadRequest, map[string]string{"error": "Файл слишком большой или повреждён."})
		return
	}
	file, _, err := request.FormFile("config")
	if err != nil {
		localui.WriteJSON(writer, http.StatusBadRequest, map[string]string{"error": "Выберите JSON-файл настроек."})
		return
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, 1024*1024+1))
	if err != nil || len(payload) > 1024*1024 {
		localui.WriteJSON(writer, http.StatusBadRequest, map[string]string{"error": "Не удалось прочитать файл настроек."})
		return
	}
	ctx, cancel := context.WithTimeout(handler.Context, 30*time.Second)
	defer cancel()
	if err := handler.Client.ImportConfig(ctx, payload); err != nil {
		localui.WriteJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) diagnostics(writer http.ResponseWriter, _ *http.Request) {
	payload, err := handler.Client.Diagnostics()
	if err != nil {
		localui.WriteJSON(writer, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writer.Header().Set("Content-Type", "application/zip")
	writer.Header().Set("Content-Disposition", `attachment; filename="InterceptClient-diagnostics.zip"`)
	writer.Write(payload)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("X-Frame-Options", "DENY")
		writer.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'")
		next.ServeHTTP(writer, request)
	})
}
