package serverui

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"intercept/internal/config"
	"intercept/internal/domain"
	"intercept/internal/interfaces/localui"
	"intercept/internal/runner"
)

//go:embed assets/*.html assets/*.css assets/*.js
//go:embed assets/telegram-guide/transport-topics.png
//go:embed assets/telegram-guide/aero-topics.png
//go:embed assets/telegram-guide/aero-general-wide.png
//go:embed assets/telegram-guide/sample-message.png
//go:embed assets/telegram-guide/botfather-create.png
var assets embed.FS

// Handler предоставляет локальному экрану только административные сценарии сервера.
type Handler struct {
	Server  *runner.Server
	Context context.Context
}

// Routes собирает экран администратора и закрытые локальные API сервера.
func (handler Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(assets, "assets")
	mux.Handle("GET /", http.FileServer(http.FS(static)))
	mux.HandleFunc("GET /api/status", func(writer http.ResponseWriter, _ *http.Request) {
		localui.WriteJSON(writer, http.StatusOK, handler.Server.Status())
	})
	mux.HandleFunc("GET /api/history/{kind}", handler.history)
	mux.HandleFunc("DELETE /api/history/{kind}", handler.clearHistory)
	mux.HandleFunc("POST /api/setup", handler.setup)
	mux.HandleFunc("GET /api/clients", handler.clients)
	mux.HandleFunc("POST /api/clients", handler.addClient)
	mux.HandleFunc("GET /api/clients/{id}/config", handler.clientConfig)
	mux.HandleFunc("GET /api/clients/{id}/package", handler.clientPackage)
	mux.HandleFunc("GET /api/destinations", func(writer http.ResponseWriter, _ *http.Request) {
		localui.WriteJSON(writer, http.StatusOK, handler.Server.SafeDestinations())
	})
	mux.HandleFunc("GET /api/template-defaults", func(writer http.ResponseWriter, _ *http.Request) {
		localui.WriteJSON(writer, http.StatusOK, handler.Server.TemplateDefaults())
	})
	mux.HandleFunc("POST /api/templates/restore-word", handler.restoreDefaultWordTemplate)
	mux.HandleFunc("POST /api/destinations", handler.addDestination)
	mux.HandleFunc("DELETE /api/destinations/{id}", handler.deleteDestination)
	mux.HandleFunc("GET /api/destinations/{id}/word-template", handler.destinationWordTemplate)
	mux.HandleFunc("GET /api/rules", handler.rules)
	mux.HandleFunc("GET /api/rule-draft", handler.ruleDraft)
	mux.HandleFunc("POST /api/rule-draft", handler.saveRuleDraft)
	mux.HandleFunc("POST /api/rules", handler.publishRules)
	mux.HandleFunc("GET /api/rule-versions", handler.ruleVersions)
	mux.HandleFunc("POST /api/rule-versions/{version}/restore", handler.restoreRules)
	mux.HandleFunc("GET /api/diagnostics", handler.diagnostics)
	mux.HandleFunc("POST /api/control/{action}", handler.control)
	return localui.SecurityHeaders(mux)
}

func (handler Handler) history(writer http.ResponseWriter, request *http.Request) {
	limit, err := strconv.Atoi(request.URL.Query().Get("limit"))
	if err != nil || limit == 0 {
		limit = 30
	}
	offset, err := strconv.Atoi(request.URL.Query().Get("offset"))
	if err != nil {
		offset = 0
	}
	page, err := handler.Server.History(request.Context(), request.PathValue("kind"), limit, offset)
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось загрузить историю.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, page)
}

func (handler Handler) clearHistory(writer http.ResponseWriter, request *http.Request) {
	if err := handler.Server.ClearHistory(request.Context(), request.PathValue("kind")); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Этот раздел нельзя очистить.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) control(writer http.ResponseWriter, request *http.Request) {
	var err error
	switch request.PathValue("action") {
	case "start":
		err = handler.Server.StartProcessing()
	case "stop":
		handler.Server.StopProcessing()
	case "refresh":
		err = handler.Server.RefreshProcessing()
	default:
		handler.publicError(writer, http.StatusNotFound, "Неизвестная команда управления.", nil)
		return
	}
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось выполнить команду. Проверьте настройки и повторите попытку.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, handler.Server.Status())
}

func (handler Handler) restoreDefaultWordTemplate(writer http.ResponseWriter, _ *http.Request) {
	path, err := handler.Server.RestoreDefaultWordTemplate()
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось восстановить стандартный шаблон Word.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]string{"path": path})
}

func (handler Handler) destinationWordTemplate(writer http.ResponseWriter, request *http.Request) {
	payload, filename, err := handler.Server.DestinationWordTemplate(request.PathValue("id"))
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось скачать шаблон Word.", err)
		return
	}
	writer.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
	base := strings.TrimSuffix(filename, ".docx")
	writer.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.docx"`, safeFilename(base)))
	writer.Write(payload)
}

func (handler Handler) clientConfig(writer http.ResponseWriter, request *http.Request) {
	payload, client, err := handler.Server.ClientConfig(request.Context(), request.PathValue("id"))
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось подготовить файл настроек рабочей точки.", err)
		return
	}
	filename := "EAVC Connect Station-" + safeFilename(client.Number) + ".json"
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	writer.Write(payload)
}

func (handler Handler) clientPackage(writer http.ResponseWriter, request *http.Request) {
	payload, client, err := handler.Server.ClientPackage(request.Context(), request.PathValue("id"))
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось подготовить готовый комплект рабочей точки.", err)
		return
	}
	filename := "EAVC Connect Station-" + safeFilename(client.Number) + ".zip"
	writer.Header().Set("Content-Type", "application/zip")
	writer.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	writer.Write(payload)
}

func (handler Handler) diagnostics(writer http.ResponseWriter, request *http.Request) {
	payload, err := handler.Server.Diagnostics(request.Context())
	if err != nil {
		handler.publicError(writer, http.StatusInternalServerError, "Не удалось подготовить диагностику. Перезапустите центр и повторите попытку.", err)
		return
	}
	writer.Header().Set("Content-Type", "application/zip")
	writer.Header().Set("Content-Disposition", `attachment; filename="EAVC-Connect-Center-diagnostics.zip"`)
	writer.Write(payload)
}

func (handler Handler) ruleVersions(writer http.ResponseWriter, request *http.Request) {
	history, err := handler.Server.RuleHistory(request.Context())
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось загрузить историю маршрутов.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, history)
}

func (handler Handler) restoreRules(writer http.ResponseWriter, request *http.Request) {
	version, err := strconv.ParseInt(request.PathValue("version"), 10, 64)
	if err != nil || version <= 0 {
		handler.publicError(writer, http.StatusBadRequest, "Выбрана некорректная версия маршрутов.", err)
		return
	}
	if err := handler.Server.RestoreRules(request.Context(), version, "Локальный администратор"); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось восстановить выбранную версию маршрутов.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) setup(writer http.ResponseWriter, request *http.Request) {
	var setup runner.TransportSetup
	if err := localui.ReadJSON(request, &setup, 1024*1024); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Проверьте заполнение полей связи.", err)
		return
	}
	ctx, cancel := context.WithTimeout(handler.Context, 30*time.Second)
	defer cancel()
	if err := handler.Server.ConfigureTransport(ctx, setup); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось проверить связь с Telegram. Проверьте токен и ID группы.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) clients(writer http.ResponseWriter, request *http.Request) {
	clients, err := handler.Server.Clients(request.Context())
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось загрузить список рабочих точек.", err)
		return
	}
	type safeClient struct {
		ID       string `json:"id"`
		Number   string `json:"number"`
		Name     string `json:"name"`
		Bot      string `json:"bot"`
		ThreadID int64  `json:"thread_id"`
		Active   bool   `json:"active"`
	}
	result := make([]safeClient, 0, len(clients))
	for _, client := range clients {
		result = append(result, safeClient{
			ID: client.ID, Number: client.Number, Name: client.Name,
			Bot:      "@" + strings.TrimPrefix(client.SenderBotUsername, "@"),
			ThreadID: client.ThreadID, Active: client.Active,
		})
	}
	localui.WriteJSON(writer, http.StatusOK, result)
}

func (handler Handler) addClient(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		Number          string `json:"number"`
		Name            string `json:"name"`
		TimeZone        string `json:"time_zone"`
		BotToken        string `json:"bot_token"`
		ThreadID        int64  `json:"thread_id"`
		CatchUpExisting bool   `json:"catch_up_existing"`
	}
	if err := localui.ReadJSON(request, &input, 1024*1024); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Проверьте заполнение полей рабочей точки.", err)
		return
	}
	ctx, cancel := context.WithTimeout(handler.Context, 30*time.Second)
	defer cancel()
	payload, err := handler.Server.AddClient(ctx, config.ClientRequest{
		Number: input.Number, Name: input.Name, EAVCPath: "auto", TimeZone: input.TimeZone,
		BotToken: input.BotToken, ThreadID: input.ThreadID, CatchUpExisting: input.CatchUpExisting,
	})
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось создать настройки рабочей точки. Проверьте токен, тему и введённые данные.", err)
		return
	}
	filename := "EAVC Connect Station-" + safeFilename(input.Number) + ".json"
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	writer.Write(payload)
}

func (handler Handler) addDestination(writer http.ResponseWriter, request *http.Request) {
	var setup runner.DestinationSetup
	if err := localui.ReadJSON(request, &setup, 1024*1024); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Проверьте заполнение полей получателя.", err)
		return
	}
	ctx, cancel := context.WithTimeout(handler.Context, 30*time.Second)
	defer cancel()
	if err := handler.Server.AddDestination(ctx, setup); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось сохранить получателя. Проверьте поля, шаблон и доступ к месту назначения.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) deleteDestination(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := context.WithTimeout(handler.Context, 30*time.Second)
	defer cancel()
	if err := handler.Server.DeleteDestination(ctx, request.PathValue("id")); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось удалить получателя. Повторите попытку.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) rules(writer http.ResponseWriter, request *http.Request) {
	rules, err := handler.Server.Rules(request.Context())
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось загрузить маршруты.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, rules)
}

func (handler Handler) ruleDraft(writer http.ResponseWriter, request *http.Request) {
	rules, exists, err := handler.Server.RuleDraft(request.Context())
	if err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось загрузить черновик маршрутов.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, struct {
		Exists bool           `json:"exists"`
		Rules  domain.RuleSet `json:"rules"`
	}{Exists: exists, Rules: rules})
}

func (handler Handler) saveRuleDraft(writer http.ResponseWriter, request *http.Request) {
	var rules domain.RuleSet
	if err := localui.ReadJSON(request, &rules, 4*1024*1024); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Проверьте настройки маршрута.", err)
		return
	}
	if err := handler.Server.SaveRuleDraft(request.Context(), rules); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось сохранить черновик маршрутов.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) publishRules(writer http.ResponseWriter, request *http.Request) {
	var rules domain.RuleSet
	if err := localui.ReadJSON(request, &rules, 4*1024*1024); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Проверьте настройки маршрутов.", err)
		return
	}
	author := strings.TrimSpace(request.Header.Get("X-Operator"))
	if author == "" {
		author = "Локальный администратор"
	}
	if err := handler.Server.PublishRules(request.Context(), rules, author); err != nil {
		handler.publicError(writer, http.StatusBadRequest, "Не удалось опубликовать маршруты.", err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) publicError(writer http.ResponseWriter, status int, message string, err error) {
	if handler.Server.Logger != nil {
		handler.Server.Logger.Warn("операция интерфейса не выполнена", "message", message, "error", err)
	}
	localui.WriteJSON(writer, status, map[string]string{"error": message})
}

func safeFilename(value string) string {
	if _, err := strconv.Atoi(value); err == nil {
		return value
	}
	return strings.Map(func(char rune) rune {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' {
			return char
		}
		return '-'
	}, value)
}
