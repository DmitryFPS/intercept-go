package clientui

import (
	"context"
	"embed"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"time"

	"intercept/internal/interfaces/localui"
	"intercept/internal/runner"
	"intercept/internal/selfupdate"
)

//go:embed assets/*
var assets embed.FS

// Handler связывает локальный экран клиента с его прикладным объектом.
type Handler struct {
	Client   *runner.Client
	Context  context.Context
	Updater  *selfupdate.Manager
	Shutdown context.CancelFunc
}

// Routes собирает экран оператора и закрытые локальные API клиента.
func (handler Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(assets, "assets")
	mux.Handle("GET /", http.FileServer(http.FS(static)))
	mux.HandleFunc("GET /api/status", func(writer http.ResponseWriter, _ *http.Request) {
		localui.WriteJSON(writer, http.StatusOK, handler.Client.Status())
	})
	mux.HandleFunc("POST /api/config/import", handler.importConfig)
	mux.HandleFunc("POST /api/eavc/search", handler.searchEAVC)
	mux.HandleFunc("POST /api/eavc/select", handler.selectEAVC)
	mux.HandleFunc("POST /api/transport/check", handler.checkTransport)
	mux.HandleFunc("POST /api/control/{action}", handler.control)
	mux.HandleFunc("GET /api/diagnostics", handler.diagnostics)
	mux.HandleFunc("GET /api/queue/{state}", handler.queueEvents)
	mux.HandleFunc("DELETE /api/queue/{state}", handler.clearQueueEvents)
	mux.HandleFunc("GET /api/update/status", handler.updateStatus)
	mux.HandleFunc("POST /api/update", handler.installUpdate)
	return localui.SecurityHeaders(mux)
}

func (handler Handler) updateStatus(writer http.ResponseWriter, _ *http.Request) {
	if handler.Updater == nil {
		localui.WriteJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "Обновление недоступно в этой сборке."})
		return
	}
	localui.WriteJSON(writer, http.StatusOK, handler.Updater.LatestResult())
}

func (handler Handler) installUpdate(writer http.ResponseWriter, request *http.Request) {
	if handler.Updater == nil || handler.Shutdown == nil {
		localui.WriteJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "Обновление недоступно в этой сборке."})
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, selfupdate.MaxPackageSize+1024*1024)
	if err := request.ParseMultipartForm(8 * 1024 * 1024); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Пакет обновления слишком большой или повреждён.", err)
		return
	}
	file, header, err := request.FormFile("update")
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Выберите подписанный пакет обновления.", err)
		return
	}
	defer file.Close()
	result, err := handler.Updater.Prepare(file, header.Size, os.Getpid())
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Пакет обновления не принят. Проверьте, что он предназначен для этой программы и получен от администратора.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusAccepted, result)
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
	time.AfterFunc(750*time.Millisecond, handler.Shutdown)
}

func (handler Handler) queueEvents(writer http.ResponseWriter, request *http.Request) {
	limit := 20
	offset := 0
	if value := request.URL.Query().Get("limit"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			limit = parsed
		}
	}
	if value := request.URL.Query().Get("offset"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			offset = parsed
		}
	}
	page, err := handler.Client.QueueEvents(request.Context(), request.PathValue("state"), limit, offset)
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось открыть список сообщений.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, page)
}

func (handler Handler) clearQueueEvents(writer http.ResponseWriter, request *http.Request) {
	count, err := handler.Client.ClearQueueEvents(request.Context(), request.PathValue("state"))
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось очистить список сообщений.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]int{"removed": count})
}

func (handler Handler) control(writer http.ResponseWriter, request *http.Request) {
	var err error
	switch request.PathValue("action") {
	case "start":
		err = handler.Client.StartProcessing()
	case "stop":
		handler.Client.StopProcessing()
	case "refresh":
		err = handler.Client.RefreshProcessing()
	default:
		handler.publicError(writer, http.StatusNotFound, "Неизвестная команда управления.", nil)
		return
	}
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось выполнить команду. Проверьте настройки и повторите попытку.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, handler.Client.Status())
}

func (handler Handler) checkTransport(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(request.Context(), 30*time.Second)
	defer cancel()
	if err := handler.Client.CheckTransport(ctx); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось проверить связь с центром. Программа продолжит попытки автоматически.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusAccepted, map[string]bool{"sent": true})
}

func (handler Handler) searchEAVC(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Extended bool `json:"extended"`
	}
	if err := localui.ReadJSON(request, &input, 16*1024); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось начать поиск. Обновите страницу и повторите попытку.", err)
		return
	}
	timeout := 20 * time.Second
	if input.Extended {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(request.Context(), timeout)
	defer cancel()
	candidates, err := handler.Client.SearchEAVC(ctx, input.Extended)
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Поиск EAVC не завершён. Повторите поиск или укажите путь вручную.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]any{"candidates": candidates})
}

func (handler Handler) selectEAVC(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Path string `json:"path"`
	}
	if err := localui.ReadJSON(request, &input, 16*1024); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Укажите путь к базе EAVC.", err)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 15*time.Second)
	defer cancel()
	if err := handler.Client.SelectEAVC(ctx, input.Path); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Выбранная база EAVC не открылась. Проверьте путь и доступ к файлу.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
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
		handler.publicError(writer, http.StatusBadRequest, "Файл настроек не принят. Скачайте его заново в центре управления.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) diagnostics(writer http.ResponseWriter, _ *http.Request) {
	payload, err := handler.Client.Diagnostics()
	if err != nil {
		handler.publicError(writer, http.StatusInternalServerError, "Не удалось подготовить диагностику. Перезапустите программу и повторите попытку.", err)
		return
	}
	writer.Header().Set("Content-Type", "application/zip")
	writer.Header().Set("Content-Disposition", `attachment; filename="EAVC-Connect-Station-diagnostics.zip"`)
	writer.Write(payload)
}

func (handler Handler) publicError(writer http.ResponseWriter, status int, message string, err error) {
	if handler.Client.Logger != nil {
		handler.Client.Logger.Warn("операция интерфейса не выполнена", "message", message, "error", err)
	}
	localui.WriteJSON(writer, status, map[string]string{"error": message})
}
