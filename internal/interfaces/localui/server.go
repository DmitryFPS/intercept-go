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
	"os/exec"
	"runtime"
	"strings"
	"time"
)

const accessHeaderName = "X-EAVC-Access"

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
		ReadTimeout: 3 * time.Minute, WriteTimeout: 3 * time.Minute,
	}
	if open {
		go func() {
			time.Sleep(300 * time.Millisecond)
			_ = OpenBrowser(AccessURL(listener.Addr().String(), accessToken))
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

// AccessURL возвращает защищённую ссылку локального интерфейса.
func AccessURL(address, accessToken string) string {
	// Фрагмент URL не передаётся HTTP-серверу и не попадает в журналы или
	// Cookie других локальных портов. JavaScript переносит его в sessionStorage.
	return "http://" + address + "/#access=" + accessToken
}

// IsRunning проверяет, принадлежит ли уже занятый адрес экземпляру программы с тем же ключом доступа.
func IsRunning(address, accessToken string) bool {
	client := &http.Client{
		Timeout: 750 * time.Millisecond,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	request, err := http.NewRequest(http.MethodGet, "http://"+address+"/api/status", nil)
	if err != nil {
		return false
	}
	request.Header.Set(accessHeaderName, accessToken)
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	return response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices
}

func protectLocalUI(authority, token string, next http.Handler) http.Handler {
	origin := "http://" + authority
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !constantEqual(request.Host, authority) {
			http.Error(writer, "Интерфейс доступен только по локальному адресу программы.", http.StatusMisdirectedRequest)
			return
		}
		if strings.HasPrefix(request.URL.Path, "/api/") && !constantEqual(request.Header.Get(accessHeaderName), token) {
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

// OpenBrowser открывает локальный адрес стандартным браузером операционной системы.
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

// WriteJSON отправляет ответ без кэширования чувствительного состояния.
func WriteJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

// SecurityHeaders добавляет единый набор ограничений для обоих локальных
// интерфейсов. Он расположен здесь, чтобы клиент и центр не расходились в
// настройках защиты при дальнейших изменениях.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("X-Frame-Options", "DENY")
		writer.Header().Set("Cache-Control", "no-store, max-age=0")
		writer.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'")
		next.ServeHTTP(writer, request)
	})
}

// ReadJSON ограничивает размер, запрещает лишние поля и посторонние данные.
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
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("после JSON найдены лишние данные")
	}
	return nil
}
