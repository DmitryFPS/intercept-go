package localui

import (
	"context"
	"crypto/subtle"
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

const accessCookieName = "intercept_local_access"

// ListenAndServe запускает интерфейс только на loopback-адресе.
func ListenAndServe(ctx context.Context, address string, handler http.Handler, open bool, accessToken string) error {
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
		Handler: protectLocalUI(listener.Addr().String(), accessToken, handler), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 90 * time.Second,
	}
	if open {
		go func() {
			time.Sleep(300 * time.Millisecond)
			_ = OpenBrowser("http://" + listener.Addr().String() + "/?access=" + url.QueryEscape(accessToken))
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

func protectLocalUI(authority, token string, next http.Handler) http.Handler {
	origin := "http://" + authority
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !constantEqual(request.Host, authority) {
			http.Error(writer, "Интерфейс доступен только по локальному адресу программы.", http.StatusMisdirectedRequest)
			return
		}
		if request.Method == http.MethodGet && request.URL.Path == "/" {
			if supplied := request.URL.Query().Get("access"); supplied != "" {
				if !constantEqual(supplied, token) {
					http.Error(writer, "Неверная ссылка запуска.", http.StatusForbidden)
					return
				}
				http.SetCookie(writer, &http.Cookie{
					Name: accessCookieName, Value: token, Path: "/", HttpOnly: true,
					SameSite: http.SameSiteStrictMode, MaxAge: 365 * 24 * 60 * 60,
				})
				http.Redirect(writer, request, "/", http.StatusSeeOther)
				return
			}
		}
		cookie, err := request.Cookie(accessCookieName)
		if err != nil || !constantEqual(cookie.Value, token) {
			http.Error(writer, "Запустите интерфейс двойным нажатием на EXE.", http.StatusUnauthorized)
			return
		}
		if request.Method != http.MethodGet && request.Method != http.MethodHead {
			if requestOrigin := request.Header.Get("Origin"); requestOrigin != origin {
				http.Error(writer, "Запрос поступил с посторонней страницы.", http.StatusForbidden)
				return
			}
			if site := request.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" {
				http.Error(writer, "Межсайтовый запрос запрещён.", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(writer, request)
	})
}

func constantEqual(left, right string) bool {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
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
	// Origin и Host проверяет единый middleware до вызова обработчика.
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("некорректный JSON: %w", err)
	}
	return nil
}
