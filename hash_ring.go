package main


// Do Init for hash ring data structure.
// Place implement consistent hashing -> Need functions for Adding Node, RemovingNode, GetNode <- implement it in Put and Get 
// Need to handle virtual nodes

import (
	"hash/crc32"
	"sort"
	"strconv"
)

func hash_method(hash_key string) uint32 {
	return crc32.ChecksumIEEE([]byte(hash_key))
}

type ConsistentHashDS struct {
	hash_ring []uint32 // Sorted hashes
	hashMap map[uint32] string // Maps hash -> server
	virtual_node_replicas int // # of virtual nodes per server
}

func newConsistentHashDS(replicas int) *ConsistentHashDS { // Makes as a new instance DS for the servers to live in.
	return &ConsistentHashDS{
		hash_ring: []uint32{},
		hashMap: make(map[uint32]string),
		virtual_node_replicas: replicas,
	}
}

func (c *ConsistentHashDS) addServer(server string) { // Adds a server to the hash_ring along with associated virtual nodes
	for i := 0; i < c.virtual_node_replicas; i++ {
		virtualNode := server + "#" + strconv.Itoa(i) // Virtual Node name of server
		h_key := hash_method(virtualNode)
		c.hash_ring = append(c.hash_ring, h_key) // Appends the key to the hash_ring
		c.hashMap[h_key] = server // Map key to server
	}
	sort.Slice(c.hash_ring, func(i, j int) bool { return c.hash_ring[i] < c.hash_ring[j] })  // Need to sort so that finds next clockwise node on ring
}

func (c *ConsistentHashDS) removeServer(server string) {  // Remove server -> key holding server needs to be mapped to next server in the ring
	newRing := []uint32{} // Build new ring with the specific server removed.
	for i := 0; i < c.virtual_node_replicas; i++ {
		virtualNode := server + "#" + strconv.Itoa(i) 
		h_key := hash_method(virtualNode)
		delete(c.hashMap, h_key)
	}
	for _, h := range c.hash_ring { // Rebuild Hash_ring after deletion
		if _, success := c.hashMap[h]; success {
			newRing = append(newRing, h)
		}
	}
	c.hash_ring = newRing
}

func (c *ConsistentHashDS) getServerbyKey(hash_key string) string {
	if len(c.hash_ring) == 0 { // No mapped server
		return ""
	}
	h := hash_method(hash_key)
	idx := sort.Search(len(c.hash_ring), func(i int) bool {return c.hash_ring[i] >= h}) // Binary search to find first hash >= hash_key
	if idx == len(c.hash_ring) {
		idx = 0 // Around in circle ring
	}
	return c.hashMap[c.hash_ring[idx]] // Server
}
