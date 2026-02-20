package main

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"strings"
)

const storeFile = "store.json"

var (
	store          = make(map[string]string)
	mu             sync.Mutex
	ErrKeyNotFound = errors.New("key not found")
)

func main() {
	if err := loadFromFile(); err != nil && !os.IsNotExist(err) {
		slog.Error("Server failed to start", "error", err)

		panic(err)
	}
	server()
	slog.Info("Server is listening on localhost:8080")

	http.ListenAndServe(":8080", nil)
}

func loadFromFile() error {
	data, err := os.ReadFile(storeFile)
	if err != nil {
		slog.Error(
			"failed to read store file",
			"file", storeFile,
			"error", err,
		)
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	slog.Info(
		"store loaded successfully",
		"file", storeFile,
		"entries", len(store),
	)
	return json.Unmarshal(data, &store)
}

func saveToFile() error {
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		slog.Error(
			"failed to marshal store",
			"entries", len(store),
			"error", err,
		)
		return err
	}
	slog.Info(
		"store saved successfully",
		"file", storeFile,
		"entries", len(store),
	)

	return os.WriteFile(storeFile, data, 0644)
}

func get(key string) (string, error) {

	mu.Lock()
	defer mu.Unlock()
	value, exists := store[key]
	if !exists {
		slog.Warn("get failed: key not found", "key", key)

		return "", ErrKeyNotFound
	}
	slog.Info("get successful", "key", key, "value", value)
	return value, nil
}

func put(key string, value string) error {
	slog.Info(
		"put request received",
		"key", key,
		"value_size", len(value),
	)
	mu.Lock()
	defer mu.Unlock()
	store[key] = value
	slog.Info("put successful", "key", key)

	return saveToFile()
}

func deleteVal(key string) error {
	mu.Lock()
	defer mu.Unlock()
	_, exists := store[key]
	if !exists {
		slog.Warn("delete failed: key not found", "key", key)

		return ErrKeyNotFound
	}
	delete(store, key)
	slog.Info("delete successful", "key", key)

	return saveToFile()
}

func server() {

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		resp := r.URL.Query().Get("key")
		slog.Info("/get called", "key", resp, "remote", r.RemoteAddr)

		if resp == "" {

			http.Error(w, "key is required and cannot be empty", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		value, err := get(resp)
		if err != nil {
			if errors.Is(err, ErrKeyNotFound) {
				http.Error(w, "key not found", http.StatusNotFound)
			} else {
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(value))
	})

	http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		key := parts[2]
		slog.Info(
			"/put called",
			"key", key,
			"remote", r.RemoteAddr,
		)
		if key == "" {
			slog.Warn("invalid put request", "key", key)

			http.Error(w, "key and value are required and cannot be empty", http.StatusBadRequest)
			return
		}

		err := put(key, value)

		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("key-value pair added successfully"))
	})

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		key := parts[2]

		slog.Info("/delete called", "key", key, "remote", r.RemoteAddr)

		if key == "" {
			http.Error(w, "key is required and cannot be empty", http.StatusBadRequest)
			return
		}
		err := deleteVal(key)
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("key-value pair deleted successfully"))
	})
}
