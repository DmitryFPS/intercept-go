package main

import (
	"archive/zip"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"intercept/internal/selfupdate"
)

func main() {
	if len(os.Args) < 2 {
		fail(errors.New("ожидается keygen, public, sign, verify или package"))
	}
	var err error
	switch os.Args[1] {
	case "keygen":
		err = keygen(os.Args[2:])
	case "public":
		err = public(os.Args[2:])
	case "sign":
		err = signFile(os.Args[2:])
	case "verify":
		err = verifyFile(os.Args[2:])
	case "package":
		err = createPackage(os.Args[2:])
	default:
		err = errors.New("неизвестная команда")
	}
	if err != nil {
		fail(err)
	}
}

func verifyFile(arguments []string) error {
	set := flag.NewFlagSet("verify", flag.ContinueOnError)
	input := set.String("input", "", "")
	signaturePath := set.String("signature", "", "")
	publicKeyText := set.String("public-key", "", "")
	if err := set.Parse(arguments); err != nil {
		return err
	}
	if *input == "" || *signaturePath == "" || *publicKeyText == "" {
		return errors.New("неполные параметры проверки подписи")
	}
	payload, err := os.ReadFile(*input)
	if err != nil {
		return err
	}
	signatureText, err := os.ReadFile(*signaturePath)
	if err != nil {
		return err
	}
	publicKey, keyErr := base64.StdEncoding.DecodeString(strings.TrimSpace(*publicKeyText))
	signature, signatureErr := base64.StdEncoding.DecodeString(strings.TrimSpace(string(signatureText)))
	if keyErr != nil || signatureErr != nil || len(publicKey) != ed25519.PublicKeySize || !ed25519.Verify(ed25519.PublicKey(publicKey), payload, signature) {
		return errors.New("подпись файла недействительна")
	}
	return nil
}

func signFile(arguments []string) error {
	set := flag.NewFlagSet("sign", flag.ContinueOnError)
	input := set.String("input", "", "")
	output := set.String("output", "", "")
	keyPath := set.String("key", "", "")
	if err := set.Parse(arguments); err != nil {
		return err
	}
	if *input == "" || *output == "" || *keyPath == "" {
		return errors.New("неполные параметры подписи")
	}
	payload, err := os.ReadFile(*input)
	if err != nil {
		return err
	}
	key, err := loadPrivate(*keyPath)
	if err != nil {
		return err
	}
	signature := base64.StdEncoding.EncodeToString(ed25519.Sign(key, payload))
	return os.WriteFile(*output, []byte(signature), 0o600)
}

func keygen(arguments []string) error {
	set := flag.NewFlagSet("keygen", flag.ContinueOnError)
	path := set.String("key", "", "")
	if err := set.Parse(arguments); err != nil {
		return err
	}
	if *path == "" {
		return errors.New("не задан путь ключа")
	}
	if _, err := os.Stat(*path); err == nil {
		return errors.New("ключ уже существует")
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(*path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(*path, []byte(base64.StdEncoding.EncodeToString(privateKey)), 0o600)
}

func public(arguments []string) error {
	set := flag.NewFlagSet("public", flag.ContinueOnError)
	path := set.String("key", "", "")
	if err := set.Parse(arguments); err != nil {
		return err
	}
	key, err := loadPrivate(*path)
	if err != nil {
		return err
	}
	fmt.Print(base64.StdEncoding.EncodeToString(key.Public().(ed25519.PublicKey)))
	return nil
}

func createPackage(arguments []string) error {
	set := flag.NewFlagSet("package", flag.ContinueOnError)
	source := set.String("source", "", "")
	product := set.String("product", "", "")
	version := set.String("version", "", "")
	output := set.String("output", "", "")
	keyPath := set.String("key", "", "")
	if err := set.Parse(arguments); err != nil {
		return err
	}
	if *source == "" || *product == "" || *version == "" || *output == "" {
		return errors.New("неполные параметры пакета")
	}
	privateKey, err := loadPrivate(*keyPath)
	if err != nil {
		return err
	}
	root, err := filepath.Abs(*source)
	if err != nil {
		return err
	}
	manifest := selfupdate.Manifest{Schema: 1, Product: *product, Version: *version}
	paths := make([]string, 0)
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("неподдерживаемый файл: %s", path)
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		relative = filepath.ToSlash(relative)
		first := strings.ToLower(strings.Split(relative, "/")[0])
		if first == "data" || first == "logs" {
			return nil
		}
		paths = append(paths, relative)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, relative := range paths {
		path := filepath.Join(root, filepath.FromSlash(relative))
		payload, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		hash := sha256.Sum256(payload)
		manifest.Files = append(manifest.Files, selfupdate.File{Path: relative, Size: int64(len(payload)), SHA256: hex.EncodeToString(hash[:])})
	}
	canonical, err := selfupdate.CanonicalManifest(manifest)
	if err != nil {
		return err
	}
	signature := base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, canonical))
	if err := os.MkdirAll(filepath.Dir(*output), 0o700); err != nil {
		return err
	}
	file, err := os.Create(*output)
	if err != nil {
		return err
	}
	archive := zip.NewWriter(file)
	write := func(name string, payload []byte) error {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		header.Modified = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
		part, createErr := archive.CreateHeader(header)
		if createErr != nil {
			return createErr
		}
		_, createErr = part.Write(payload)
		return createErr
	}
	pretty, _ := json.MarshalIndent(manifest, "", "  ")
	if err = write(selfupdate.ManifestName, pretty); err == nil {
		err = write(selfupdate.SignatureName, []byte(signature))
	}
	for _, relative := range paths {
		if err != nil {
			break
		}
		input, openErr := os.Open(filepath.Join(root, filepath.FromSlash(relative)))
		if openErr != nil {
			err = openErr
			break
		}
		header := &zip.FileHeader{Name: "payload/" + relative, Method: zip.Deflate}
		header.Modified = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
		part, createErr := archive.CreateHeader(header)
		if createErr == nil {
			_, createErr = io.Copy(part, input)
		}
		_ = input.Close()
		err = createErr
	}
	closeArchive := archive.Close()
	closeFile := file.Close()
	return errors.Join(err, closeArchive, closeFile)
}

func loadPrivate(path string) (ed25519.PrivateKey, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(payload)))
	if err != nil || len(decoded) != ed25519.PrivateKeySize {
		return nil, errors.New("некорректный закрытый ключ обновлений")
	}
	return ed25519.PrivateKey(decoded), nil
}

func fail(err error) { _, _ = fmt.Fprintln(os.Stderr, err); os.Exit(1) }
