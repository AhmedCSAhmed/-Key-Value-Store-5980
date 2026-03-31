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
	"flag"
)

const storeFile = "store.bin"


type ServerNode struct {
	name string 
	node_store map[string] string 
	mu sync.RWMutex
}

var (
	server_nodes []*ServerNode // All server nodes
	con_hash *ConsistentHashDS
)

var ErrKeyNotFound = errors.New("key not found")

// Run docker for KV Store
// docker build -t kvstore:latest .
// docker run -d -e NODE_NAME=kvNode1 -e PORT=8091 --name kv1 -p 8091:8091 kvstore:latest
// docker run -d -e NODE_NAME=kvNode2 -e PORT=8092 --name kv2 -p 8092:8092 kvstore:latest
// docker run -d -e NODE_NAME=kvNode3 -e PORT=8093 --name kv3 -p 8093:8093 kvstore:latest

func main() {
	
	nodes := []*ServerNode {
		{name: "kvNode1", node_store: make(map[string]string)},
		{name: "kvNode2", node_store: make(map[string]string)},
		{name: "kvNode3", node_store: make(map[string]string)},
	}
	server_nodes = nodes
	con_hash = newConsistentHashDS(3) // 3 node instances
	for _, n := range nodes {
		if err := n.loadFromFile(); err != nil && !os.IsNotExist(err){
		slog.Error("failed to load node store", "node", n.name, "error", err)
		}
		con_hash.addServer(n.name)
	}
	server()
	port := flag.String("port", "8090", "port to listen on")
    flag.Parse()
	addr := ":" + *port
	slog.Info("Server is listening on", "port", *port)

 	if err := http.ListenAndServe(addr, nil); err != nil {
        slog.Error("Server failed", "error", err)
	}
}

func (n *ServerNode) loadFromFile() error {
	f, err := os.Open(n.name + ".bin")
	if err != nil {
		return err
	}
	defer f.Close()
	n.mu.Lock()
	defer n.mu.Unlock()
	if err = gob.NewDecoder(f).Decode(&n.node_store); err != nil {
		return err
	}
	slog.Info("node store loaded", "node", n.name, "node entries", len(n.node_store))
	return nil
}

func (n *ServerNode) saveToFile() error {
	f, err := os.Create(n.name + ".bin")
	if err != nil {
		return err
	}
	defer f.Close()
	if err := gob.NewEncoder(f).Encode(n.node_store); err != nil {
		return err
	}
	slog.Info("node store saved", "node", n.name, "node entries", len(n.node_store))
	return nil
}

func getServerKey(server_key string, nodes []*ServerNode) *ServerNode {
	serverName := con_hash.getServerbyKey(server_key)
	for _, n := range nodes {
		if n.name == serverName {
			return n
		}
	}
	return nil


}

func get(key string, nodes []*ServerNode) (string, error) {
	n := getServerKey(key, nodes)
	if n == nil {
		return "", errors.New("no node found for key")
	}

	n.mu.RLock()
	defer n.mu.RUnlock()
	value, exists := n.node_store[key]
	if !exists {
		slog.Warn("get failed: key not found", "key", key)
		return "", ErrKeyNotFound
	}
	slog.Info("get successful", "key", key, "value", value)
	return value, nil
}

func put(key string, value string, nodes []*ServerNode) error {
	n := getServerKey(key, nodes)
	if n == nil {
		return errors.New("no node found for key")
	}

	slog.Info(
		"put request received",
		"key", key,
		"value_size", len(value),
		"node", n.name,
	)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.node_store[key] = value
	slog.Info("put successful", "key", key, "node", n.name)

	if err := n.saveToFile(); err != nil {
		slog.Error("failed to save node store", "node", n.name, "error", err)
		return err
	}
	return nil
}

func deleteVal(key string, nodes []*ServerNode) error {
	n := getServerKey(key, nodes)
	if n == nil {
		return errors.New("no node found for key")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	_, exists := n.node_store[key]
	if !exists {
		slog.Warn("delete failed: key not found", "key", key)

		return ErrKeyNotFound
	}
	delete(n.node_store, key)
	slog.Info("delete successful", "key", key)

	return n.saveToFile()
}

func server() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		if key == "" {
			return 
		}
		defer r.Body.Close()
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			value, err := get(key, server_nodes)
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
			if err := put(key, payload.Value, server_nodes); err != nil {
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
		value, err := get(key, server_nodes)
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
		if err := put(key, value, server_nodes); err != nil {
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
		if err := deleteVal(key, server_nodes); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("key-value pair deleted successfully"))
	})
}
