package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"golang.org/x/sys/unix"
)

const (
	storeMmapFile = "store.mmap"    // mmap since can modify in-disk memory compared to doing a full file rewrite and no encode and decode (O(1) appending)
	maxSize       = 1024 * 1024 * 8 // Max size for virtual address space in mmap file when mapping
)

var (
	idx             = map[string]int{} // key -> offset
	writingPosition int                // Append offset for specifically finding starting point of .mmap file
	mu              sync.Mutex
	ErrKeyNotFound  = errors.New("key not found")
	data            []byte // mmap'd file (file on disk assigned within virtual memory by an address range)
)

func main() {
	err := initalize_map()
	if err != nil && !os.IsNotExist(err) {
		slog.Error("Failed to initalize mem mapping", "error", err)

		panic(err)
	}
	server()
	slog.Info("Server is listening on localhost:8090")

	http.ListenAndServe(":8090", nil)
}

func initalize_map() error { // Setup mmap permissions and file
	file, err := os.OpenFile(storeMmapFile, os.O_RDWR|os.O_CREATE, 0644) // 644 - Root User (r and w), Rest (reads) and creation of file on OS
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(maxSize); err != nil { // shrinks the file to address maxSize
		return err
	}
	data, err = syscall.Mmap(
		int(file.Fd()),
		// File descriptor in file
		0,
		maxSize,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED, // Changes are shared with other processes mapping into the same file, later written to disk file
	)
	if err != nil {
		return err
	}
	fix_Idx()
	slog.Info("mmap initialized", "entries", len(idx))
	return nil

}

func fix_Idx() { // Recover Idx in file when writing during the mapping process after kv-store operations
	file_position := 0
	for file_position+8 <= maxSize {
		key_len := int(binary.LittleEndian.Uint32(data[file_position:]))
		value_len := int(binary.LittleEndian.Uint32(data[file_position+4:]))
		if key_len == 0 && value_len == 0 {
			break
		}
		key_start := file_position + 8
		value_start := key_start + key_len // After writing the key thats where start writing value of that kv pair
		key := string(data[key_start : key_start+key_len])
		idx[key] = file_position
		file_position = value_start + value_len // After writing kv pair and doing operation
	}
	writingPosition = file_position // Rebuilding spot where a new kv pair would be written to file

}

func get(key string) (string, error) {

	mu.Lock()
	defer mu.Unlock()

	off, ok := idx[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	klen := int(binary.LittleEndian.Uint32(data[off:]))
	vlen := int(binary.LittleEndian.Uint32(data[off+4:]))

	valStart := off + 8 + klen
	value := string(data[valStart : valStart+vlen]) // Actual val in KV store

	slog.Info("get request called", "key", key, "value", value, "Length", len(idx))

	return string(data[valStart : valStart+vlen]), nil
}

func put(key string, value string) error {
	mu.Lock()
	defer mu.Unlock()

	klen := len(key)
	vlen := len(value)
	recordSize := 8 + klen + vlen

	if writingPosition+recordSize > maxSize {
		return errors.New("store full")
	}

	binary.LittleEndian.PutUint32(data[writingPosition:], uint32(klen))
	binary.LittleEndian.PutUint32(data[writingPosition+4:], uint32(vlen))
	copy(data[writingPosition+8:], key)
	copy(data[writingPosition+8+klen:], value)

	idx[key] = writingPosition
	writingPosition += recordSize
	slog.Info(
		"put request called",
		"key", key,
		"value", value,
		"size", len(idx),
	)
	if err := unix.Msync(data, unix.MS_SYNC); err != nil { // Synchronizes mmaps with disk storage and makes sure writes to actual file
		slog.Error("msync failed", "error", err)
		return err
	}
	return nil
}

func deleteVal(key string) error {
	mu.Lock()
	defer mu.Unlock()
	delete(idx, key)
	slog.Info("delete successful", "key", key)

	return nil
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
