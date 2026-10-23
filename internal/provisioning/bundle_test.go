package provisioning

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestBundleRequiresSeparateCodeAndDetectsTampering(t *testing.T) {
	plain := []byte(`{"bot_token":"secret","key":"also-secret"}`)
	bundle, code, err := Seal(plain, bytes.NewReader(bytes.Repeat([]byte{7}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(bundle, []byte("secret")) || bytes.Contains(bundle, plain) {
		t.Fatal("секреты остались в открытом комплекте")
	}
	opened, err := Open(bundle, code)
	if err != nil || !bytes.Equal(opened, plain) {
		t.Fatalf("комплект не открылся: %q, %v", opened, err)
	}
	if _, err := Open(bundle, "AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-AA"); err == nil {
		t.Fatal("неверный код принят")
	}
	var parsed Bundle
	if err := json.Unmarshal(bundle, &parsed); err != nil {
		t.Fatal(err)
	}
	parsed.Ciphertext = "AA" + parsed.Ciphertext[2:]
	tampered, _ := json.Marshal(parsed)
	if _, err := Open(tampered, code); err == nil {
		t.Fatal("изменённый комплект принят")
	}
}
