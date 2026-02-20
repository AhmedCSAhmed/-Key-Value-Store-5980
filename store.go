package main
import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"sync"
)

const storeFile = "store.json"

var (
	store         = make(map[string]string)
	mu            sync.Mutex
	ErrKeyNotFound = errors.New("key not found")
)

func main() {
	if err := loadFromFile(); err != nil && !os.IsNotExist(err) {
		panic(err)
	}
	server()
	http.ListenAndServe(":8090", nil)
}

func loadFromFile() error {
	data, err := os.ReadFile(storeFile)
	if err != nil {
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	return json.Unmarshal(data, &store)
}

func saveToFile() error {
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(storeFile, data, 0644)
}


func get(key string) (string, error) {
	mu.Lock()
	defer mu.Unlock()
	value, exists := store[key]
	if !exists {
		return "", ErrKeyNotFound
	}
	return value, nil
}

func put(key string, value string) error {
	mu.Lock()
	defer mu.Unlock()
	store[key] = value
	return saveToFile()
}

func deleteVal(key string) error {
	mu.Lock()
	defer mu.Unlock()
	_, exists := store[key];
	if !exists {
		return ErrKeyNotFound
	}
	delete(store, key)
	return saveToFile()
}




func server(){

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request){
		resp  := r.URL.Query().Get("key")
		if resp == "" {
			http.Error(w,"key is required and cannot be empty", http.StatusBadRequest)
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

http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request){
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if key == "" || value == "" {
		http.Error(w,"key and value are required and cannot be empty", http.StatusBadRequest)
		return 
	}
	
	err := put(key, value) 

	if err != nil {
		http.Error(w,"internal server error",http.StatusInternalServerError)
		return 
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("key-value pair added successfully"))
})

http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request){
	key := r.URL.Query().Get("key")
	if key == ""{
		http.Error(w,"key is required and cannot be empty", http.StatusBadRequest)
		return 
	}
	err := deleteVal(key)
	if err != nil {
		http.Error(w,"internal server error",http.StatusInternalServerError)
		return 
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("key-value pair deleted successfully"))
	})
}

