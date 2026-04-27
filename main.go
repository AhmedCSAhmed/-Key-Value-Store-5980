package main

import (
	"flag"
	"log"
	"net/http"
	"os"
)

var port = "8090"

var (
	server_nodes []*ServerNode
	con_hash     *ConsistentHashDS
)

func main() {
    port := flag.String("port", "8090", "port to listen on")
	nodeName := flag.String("node", "kvNode1", "node name") // ✅ ADD THIS

	flag.Parse()

	if envPort := os.Getenv("PORT"); envPort != "" {
		*port = envPort
	}
	node := newServerNode(*nodeName)

	server_nodes = []*ServerNode{}
	con_hash = newConsistentHashDS(3)

	server_nodes = append(server_nodes, node)
	nodeMap[node.name] = node
	con_hash.addServer(node.name)


	nodeMap = make(map[string]*ServerNode)
	for _, n := range server_nodes {
		nodeMap[n.name] = n
	}

	mux := server(node)

	log.Println("server running on", *port)
	log.Fatal(http.ListenAndServe(":"+*port, mux))
}