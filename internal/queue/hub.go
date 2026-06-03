package queue

import "sync"

type Hub struct {
	mu      sync.Mutex
	clients map[string]*Client
	rooms   map[string]map[string]*Client
}

func NewHub() *Hub {
	return &Hub{clients: map[string]*Client{}, rooms: map[string]map[string]*Client{}}
}

func (h *Hub) Add(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c.ID] = c
}

func (h *Hub) Get(id string) *Client {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.clients[id]
}

func (h *Hub) Remove(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for room := range c.Rooms {
		if h.rooms[room] != nil {
			delete(h.rooms[room], c.ID)
		}
	}
	delete(h.clients, c.ID)
}

func (h *Hub) Join(c *Client, room string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c.Rooms[room] = true
	if h.rooms[room] == nil {
		h.rooms[room] = map[string]*Client{}
	}
	h.rooms[room][c.ID] = c
}

func (h *Hub) Clients(room string) []*Client {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := []*Client{}
	for _, c := range h.rooms[room] {
		out = append(out, c)
	}
	return out
}
