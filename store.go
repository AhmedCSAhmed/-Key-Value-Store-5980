package main

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
)

const storeFile = "store.bin"

var (
	store = make(map[string]string)
	mu sync.Mutex
	ErrKeyNotFound = errors.New("key not found")
)

func main() {
	err := loadFromFile()
	if err != nil && !os.IsNotExist(err) {
		slog.Error("Server failed to start", "error", err)

		panic(err)
	}
	server()
	slog.Info("Server is listening on localhost:8090")

	http.ListenAndServe(":8090", nil)
}

func loadFromFile() error {
	f, err := os.Open(storeFile)
	if err != nil {
		return err
	}
	defer f.Close()
	mu.Lock()
	defer mu.Unlock()
	if err = gob.NewDecoder(f).Decode(&store); err != nil {
		return err
	}
	slog.Info("store loaded from file", "file", storeFile, "entries", len(store))
	return nil
}

func saveToFile() error {
	// Caller holds mu
	f, err := os.Create(storeFile)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := gob.NewEncoder(f).Encode(store); err != nil {
		return err
	}
	slog.Info("store saved to file", "file", storeFile, "entries", len(store))
	return nil
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
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		if key == "" {
			return 
		}
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			value, err := get(key)
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
		case http.MethodPost:
			var payload struct {
				Value string `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			if err := put(key, payload.Value); err != nil {
				http.Error(w, "internal server error", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
		}
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key is required and cannot be empty", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		value, err := get(key)
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
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		if key == "" || value == "" {
			http.Error(w, "key and value are required and cannot be empty", http.StatusBadRequest)
			return
		}
		if err := put(key, value); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("key-value pair added successfully"))
	})

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key is required and cannot be empty", http.StatusBadRequest)
			return
		}
		if err := deleteVal(key); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("key-value pair deleted successfully"))
	})
}
