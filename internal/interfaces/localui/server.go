package localui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// ListenAndServe запускает интерфейс только на loopback-адресе.
func ListenAndServe(ctx context.Context, address string, handler http.Handler, open bool) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("запустить локальный интерфейс: %w", err)
	}
	host, _, err := net.SplitHostPort(listener.Addr().String())
	if err != nil || (host != "127.0.0.1" && host != "::1") {
		listener.Close()
		return errors.New("интерфейс разрешено открывать только на этом компьютере")
	}
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 90 * time.Second,
	}
	if open {
		go func() {
			time.Sleep(300 * time.Millisecond)
			_ = OpenBrowser("http://" + listener.Addr().String())
		}()
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func OpenBrowser(address string) error {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		command = exec.Command("rundll32", "url.dll,FileProtocolHandler", address)
	case "darwin":
		command = exec.Command("open", address)
	default:
		command = exec.Command("xdg-open", address)
	}
	return command.Start()
}

func WriteJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func ReadJSON(request *http.Request, target any, maxBytes int64) error {
	if request.Method != http.MethodPost && request.Method != http.MethodPut {
		return errors.New("метод не поддерживается")
	}
	if !strings.HasPrefix(strings.ToLower(request.Header.Get("Content-Type")), "application/json") {
		return errors.New("ожидается JSON")
	}
	if origin := request.Header.Get("Origin"); origin != "" {
		parsed, err := url.Parse(origin)
		if err != nil || !strings.EqualFold(parsed.Host, request.Host) {
			return errors.New("запрос поступил с посторонней страницы")
		}
	}
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("некорректный JSON: %w", err)
	}
	return nil
}
