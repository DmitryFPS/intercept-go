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

//go:embed assets/*
var assets embed.FS

type Handler struct {
	Server  *runner.Server
	Context context.Context
}

func (handler Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(assets, "assets")
	mux.Handle("GET /", http.FileServer(http.FS(static)))
	mux.HandleFunc("GET /api/status", func(writer http.ResponseWriter, _ *http.Request) {
		localui.WriteJSON(writer, http.StatusOK, handler.Server.Status())
	})
	mux.HandleFunc("POST /api/setup", handler.setup)
	mux.HandleFunc("GET /api/clients", handler.clients)
	mux.HandleFunc("POST /api/clients", handler.addClient)
	mux.HandleFunc("GET /api/destinations", func(writer http.ResponseWriter, _ *http.Request) {
		localui.WriteJSON(writer, http.StatusOK, handler.Server.SafeDestinations())
	})
	mux.HandleFunc("POST /api/destinations", handler.addDestination)
	mux.HandleFunc("GET /api/rules", handler.rules)
	mux.HandleFunc("POST /api/rules", handler.publishRules)
	mux.HandleFunc("GET /api/rule-versions", handler.ruleVersions)
	mux.HandleFunc("POST /api/rule-versions/{version}/restore", handler.restoreRules)
	return securityHeaders(mux)
}

func (handler Handler) ruleVersions(writer http.ResponseWriter, request *http.Request) {
	history, err := handler.Server.RuleHistory(request.Context())
	if err != nil {
		badRequest(writer, err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, history)
}

func (handler Handler) restoreRules(writer http.ResponseWriter, request *http.Request) {
	version, err := strconv.ParseInt(request.PathValue("version"), 10, 64)
	if err != nil || version <= 0 {
		badRequest(writer, fmt.Errorf("некорректная версия маршрутов"))
		return
	}
	if err := handler.Server.RestoreRules(request.Context(), version, "Локальный администратор"); err != nil {
		badRequest(writer, err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) setup(writer http.ResponseWriter, request *http.Request) {
	var setup runner.TransportSetup
	if err := localui.ReadJSON(request, &setup, 1024*1024); err != nil {
		badRequest(writer, err)
		return
	}
	ctx, cancel := context.WithTimeout(handler.Context, 30*time.Second)
	defer cancel()
	if err := handler.Server.ConfigureTransport(ctx, setup); err != nil {
		badRequest(writer, err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) clients(writer http.ResponseWriter, request *http.Request) {
	clients, err := handler.Server.Clients(request.Context())
	if err != nil {
		badRequest(writer, err)
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
		EAVCPath        string `json:"eavc_path"`
		BotToken        string `json:"bot_token"`
		ThreadID        int64  `json:"thread_id"`
		CatchUpExisting bool   `json:"catch_up_existing"`
	}
	if err := localui.ReadJSON(request, &input, 1024*1024); err != nil {
		badRequest(writer, err)
		return
	}
	ctx, cancel := context.WithTimeout(handler.Context, 30*time.Second)
	defer cancel()
	payload, err := handler.Server.AddClient(ctx, config.ClientRequest{
		Number: input.Number, Name: input.Name, EAVCPath: input.EAVCPath,
		BotToken: input.BotToken, ThreadID: input.ThreadID, CatchUpExisting: input.CatchUpExisting,
	})
	if err != nil {
		badRequest(writer, err)
		return
	}
	filename := "InterceptClient-" + safeFilename(input.Number) + ".json"
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	writer.Write(payload)
}

func (handler Handler) addDestination(writer http.ResponseWriter, request *http.Request) {
	var setup runner.DestinationSetup
	if err := localui.ReadJSON(request, &setup, 1024*1024); err != nil {
		badRequest(writer, err)
		return
	}
	ctx, cancel := context.WithTimeout(handler.Context, 30*time.Second)
	defer cancel()
	if err := handler.Server.AddDestination(ctx, setup); err != nil {
		badRequest(writer, err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func (handler Handler) rules(writer http.ResponseWriter, request *http.Request) {
	rules, err := handler.Server.Rules(request.Context())
	if err != nil {
		badRequest(writer, err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, rules)
}

func (handler Handler) publishRules(writer http.ResponseWriter, request *http.Request) {
	var rules domain.RuleSet
	if err := localui.ReadJSON(request, &rules, 4*1024*1024); err != nil {
		badRequest(writer, err)
		return
	}
	author := strings.TrimSpace(request.Header.Get("X-Operator"))
	if author == "" {
		author = "Локальный администратор"
	}
	if err := handler.Server.PublishRules(request.Context(), rules, author); err != nil {
		badRequest(writer, err)
		return
	}
	localui.WriteJSON(writer, http.StatusOK, map[string]bool{"ok": true})
}

func badRequest(writer http.ResponseWriter, err error) {
	localui.WriteJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
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

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("X-Frame-Options", "DENY")
		writer.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'")
		next.ServeHTTP(writer, request)
	})
}
