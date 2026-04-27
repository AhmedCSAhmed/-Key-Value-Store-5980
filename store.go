package main

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"strings"
	"encoding/json" 
)

// Run docker for KV Store
// docker build -t kvstore:latest .
// docker run -d \
//   -e NODE_NAME=kvNode1 \
//   -e PORT=8091 \
//   -e PEERS=kvNode1,kvNode2,kvNode3 \
//   -p 8091:8091 kvstore ./kvserver --node kvNode1 --port 8091

//   docker run -d \
//   -e NODE_NAME=kvNode2 \
//   -e PORT=8092 \
//   -e PEERS=kvNode1,kvNode2,kvNode3 \
//   -p 8092:8092 kvstore ./kvserver --node kvNode2 --port 8092

//   docker run -d \
//   -e NODE_NAME=kvNode3 \
//   -e PORT=8093 \
//   -e PEERS=kvNode1,kvNode2,kvNode3 \
//   -p 8093:8093 kvstore ./kvserver --node kvNode3 --port 8093


// docker logs kv1
// docker logs kv2
// docker logs kv3
// Steps before demo:
// 1. Possible move to TCP over HTTP
// 2. Query param over JSON 
// 3. Batch Processing

var ErrKeyNotFound = errors.New("key not found")

const numShards = 32
var (
	nodeMapMu sync.RWMutex
	nodeMap   = make(map[string]*ServerNode)
)
type shard struct {
	m  map[string]string
	mu sync.RWMutex
}

type ServerNode struct {
	name string

	shards []shard

	flushInterval any 
	dirty         atomic.Bool
	persistCh     chan struct{}
	stopCh        chan struct{}
	doneCh        chan struct{}
}




func newServerNode(name string) *ServerNode {
	shards := make([]shard, numShards)
	for i := range shards {
		shards[i].m = make(map[string]string)
	}

	return &ServerNode{
		name:   name,
		shards: shards,
	}
}


func (n *ServerNode) shardIndex(key string) int {
	return int(hash_method(key)) % numShards
}


func route(key string) *ServerNode {
	nodeName := con_hash.getServerbyKey(key)

	nodeMapMu.RLock()
	defer nodeMapMu.RUnlock()

	return nodeMap[nodeName]
}



func get(key string, nodes []*ServerNode) (string, error) {
	n := route(key)
	if n == nil {
		return "", errors.New("no node found")
	}

	s := &n.shards[n.shardIndex(key)]

	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.m[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return val, nil
}


func put(key, value string, nodes []*ServerNode) error {
	n := route(key)
	if n == nil {
		return errors.New("no node found")
	}

	s := &n.shards[n.shardIndex(key)]

	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = value

	return nil 
}


func deleteVal(key string, nodes []*ServerNode) error {
	n := route(key)
	if n == nil {
		return errors.New("no node found")
	}

	s := &n.shards[n.shardIndex(key)]

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.m[key]; !ok {
		return ErrKeyNotFound
	}

	delete(s.m, key)
	return nil
}

func server(node *ServerNode) *http.ServeMux {
	mux := http.NewServeMux()

	// KV handler: /<key>
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		// avoid accidentally catching /healthz etc.
		if r.URL.Path == "/" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		switch r.Method {

		case http.MethodGet:
			val, err := get(key, server_nodes)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(val))

		case http.MethodPost:
			var body struct {
				Value string `json:"value"`
			}

			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "invalid json body", http.StatusBadRequest)
				return
			}

			if body.Value == "" {
				http.Error(w, "missing value", http.StatusBadRequest)
				return
			}

			if err := put(key, body.Value, server_nodes); err != nil {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))

		case http.MethodDelete:
			if err := deleteVal(key, server_nodes); err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("deleted"))

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","node":"` + node.name + `"}`))
	})

	return mux
}