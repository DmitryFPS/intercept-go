package provisioning

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	Schema     = "eavc-connect-activation/v1"
	Iterations = 200000
)

type Bundle struct {
	Schema     string `json:"schema"`
	Salt       string `json:"salt"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

// Seal шифрует первоначальные настройки отдельным высокоэнтропийным кодом.
// Код намеренно не записывается в комплект и должен передаваться отдельно.
func Seal(payload []byte, random io.Reader) ([]byte, string, error) {
	if len(payload) == 0 {
		return nil, "", errors.New("пустые данные активации")
	}
	if random == nil {
		random = rand.Reader
	}
	secret := make([]byte, 16)
	salt := make([]byte, 16)
	if _, err := io.ReadFull(random, secret); err != nil {
		return nil, "", err
	}
	if _, err := io.ReadFull(random, salt); err != nil {
		return nil, "", err
	}
	code := formatCode(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(secret))
	key := pbkdf2SHA256([]byte(normalizeCode(code)), salt, Iterations, 32)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(random, nonce); err != nil {
		return nil, "", err
	}
	ciphertext := gcm.Seal(nil, nonce, payload, []byte(Schema))
	bundle, err := json.MarshalIndent(Bundle{
		Schema: Schema, Salt: base64.RawStdEncoding.EncodeToString(salt),
		Nonce:      base64.RawStdEncoding.EncodeToString(nonce),
		Ciphertext: base64.RawStdEncoding.EncodeToString(ciphertext),
	}, "", "  ")
	if err != nil {
		return nil, "", err
	}
	return bundle, code, nil
}

// Open расшифровывает комплект только точным кодом активации.
func Open(payload []byte, code string) ([]byte, error) {
	var bundle Bundle
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return nil, errors.New("файл активации повреждён")
	}
	if bundle.Schema != Schema || len(normalizeCode(code)) != 26 {
		return nil, errors.New("неверный код или формат файла активации")
	}
	salt, err := base64.RawStdEncoding.DecodeString(bundle.Salt)
	if err != nil || len(salt) != 16 {
		return nil, errors.New("файл активации повреждён")
	}
	nonce, err := base64.RawStdEncoding.DecodeString(bundle.Nonce)
	if err != nil {
		return nil, errors.New("файл активации повреждён")
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(bundle.Ciphertext)
	if err != nil {
		return nil, errors.New("файл активации повреждён")
	}
	key := pbkdf2SHA256([]byte(normalizeCode(code)), salt, Iterations, 32)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil || len(nonce) != gcm.NonceSize() {
		return nil, errors.New("файл активации повреждён")
	}
	plain, err := gcm.Open(nil, nonce, ciphertext, []byte(Schema))
	if err != nil {
		return nil, errors.New("неверный код или файл активации изменён")
	}
	return plain, nil
}

func normalizeCode(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	value = strings.NewReplacer("-", "", " ", "", "\t", "", "\r", "", "\n", "").Replace(value)
	return value
}

func formatCode(value string) string {
	var groups []string
	for len(value) > 0 {
		size := 4
		if len(value) < size {
			size = len(value)
		}
		groups = append(groups, value[:size])
		value = value[size:]
	}
	return strings.Join(groups, "-")
}

func pbkdf2SHA256(password, salt []byte, iterations, length int) []byte {
	result := make([]byte, 0, length)
	for blockIndex := uint32(1); len(result) < length; blockIndex++ {
		mac := hmac.New(sha256.New, password)
		_, _ = mac.Write(salt)
		var counter [4]byte
		binary.BigEndian.PutUint32(counter[:], blockIndex)
		_, _ = mac.Write(counter[:])
		u := mac.Sum(nil)
		block := append([]byte(nil), u...)
		for iteration := 1; iteration < iterations; iteration++ {
			mac = hmac.New(sha256.New, password)
			_, _ = mac.Write(u)
			u = mac.Sum(nil)
			for index := range block {
				block[index] ^= u[index]
			}
		}
		result = append(result, block...)
	}
	return result[:length]
}

func ValidateCode(code string) error {
	decoded, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(normalizeCode(code))
	if err != nil || len(decoded) != 16 {
		return fmt.Errorf("код активации должен содержать 26 знаков")
	}
	return nil
}
