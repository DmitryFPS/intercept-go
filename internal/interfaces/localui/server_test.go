package localui

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// После одного JSON-объекта тело должно закончиться, иначе запрос мог быть
// сформирован двусмысленно или содержать случайно склеенные настройки.
func TestReadJSONRejectsTrailingObject(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"value":1}{"value":2}`))
	request.Header.Set("Content-Type", "application/json")
	var target struct {
		Value int `json:"value"`
	}
	if err := ReadJSON(request, &target, 1024); err == nil {
		t.Fatal("лишний JSON не был отклонён")
	}
}

// Панель управления открывается только локально и только по выданной программой ссылке с секретным ключом.
func TestProtectLocalUI(t *testing.T) {
	const (
		authority = "127.0.0.1:17631"
		token     = "0123456789abcdef0123456789abcdef"
	)
	handler := protectLocalUI(authority, token, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodGet, "http://"+authority+"/api/status", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("ожидался 401 без локального токена, получен %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "http://evil.example/", nil)
	request.Host = "evil.example"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMisdirectedRequest {
		t.Fatalf("ожидался 421 для подменённого Host, получен %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "http://"+authority+"/", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("ожидалась доступная статическая оболочка, получен %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "http://"+authority+"/api/status", nil)
	request.Header.Set(accessHeaderName, token)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("ожидался доступ с заголовком сеанса, получен %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "http://"+authority+"/api/action", nil)
	request.Header.Set(accessHeaderName, token)
	request.Header.Set("Origin", "http://attacker.example")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("ожидался 403 для чужого Origin, получен %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "http://"+authority+"/api/action", nil)
	request.Header.Set(accessHeaderName, token)
	request.Header.Set("Origin", "http://"+authority)
	request.Header.Set("Sec-Fetch-Site", "same-origin")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("ожидался доступ для same-origin POST, получен %d", response.Code)
	}
}

func TestIsRunningRecognizesSameProtectedInterface(t *testing.T) {
	const token = "0123456789abcdef0123456789abcdef"
	server := httptest.NewUnstartedServer(nil)
	authority := server.Listener.Addr().String()
	server.Config.Handler = protectLocalUI(authority, token, http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	server.Start()
	defer server.Close()

	if !IsRunning(authority, token) {
		t.Fatal("работающий интерфейс с тем же ключом не распознан")
	}
	if IsRunning(authority, "другой-ключ") {
		t.Fatal("интерфейс с другим ключом ошибочно распознан как свой")
	}
}
