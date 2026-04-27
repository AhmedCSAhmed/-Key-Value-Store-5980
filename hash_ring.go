package main

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

func hash_method(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

type ConsistentHashDS struct {
	mu sync.RWMutex

	hash_ring []uint32
	hashMap   map[uint32]string

	virtual_node_replicas int
}

func newConsistentHashDS(replicas int) *ConsistentHashDS {
	return &ConsistentHashDS{
		hash_ring:             []uint32{},
		hashMap:               make(map[uint32]string),
		virtual_node_replicas: replicas,
	}
}

func (c *ConsistentHashDS) addServer(server string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := 0; i < c.virtual_node_replicas; i++ {
		virtualNode := server + "#" + strconv.Itoa(i)
		h := hash_method(virtualNode)

		c.hash_ring = append(c.hash_ring, h)
		c.hashMap[h] = server
	}

	sort.Slice(c.hash_ring, func(i, j int) bool {
		return c.hash_ring[i] < c.hash_ring[j]
	})
}

func (c *ConsistentHashDS) removeServer(server string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newRing := make([]uint32, 0, len(c.hash_ring))

	for i := 0; i < c.virtual_node_replicas; i++ {
		virtualNode := server + "#" + strconv.Itoa(i)
		h := hash_method(virtualNode)
		delete(c.hashMap, h)
	}

	for _, h := range c.hash_ring {
		if _, ok := c.hashMap[h]; ok {
			newRing = append(newRing, h)
		}
	}

	c.hash_ring = newRing
}

func (c *ConsistentHashDS) getServerbyKey(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.hash_ring) == 0 {
		return ""
	}

	h := hash_method(key)

	idx := sort.Search(len(c.hash_ring), func(i int) bool {
		return c.hash_ring[i] >= h
	})

	if idx == len(c.hash_ring) {
		idx = 0
	}

	return c.hashMap[c.hash_ring[idx]]
}