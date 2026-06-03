package queue

import (
	"encoding/json"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	ID      string
	UserID  string
	Claims  userClaims
	Conn    *websocket.Conn
	Rooms   map[string]bool
	send    func(string, any)
	closed  bool
	closeFn func(int, string)
}

func NewTestClient(id, userID string) *Client {
	return &Client{ID: id, UserID: userID, Rooms: map[string]bool{}}
}

func (c *Client) Send(t string, payload any) {
	if c.send != nil {
		c.send(t, payload)
		return
	}
	if c.Conn != nil {
		_ = c.Conn.WriteJSON(map[string]any{"type": t, "payload": payload})
	}
}

func (c *Client) Close(code int, reason string) {
	c.closed = true
	if c.closeFn != nil {
		c.closeFn(code, reason)
		return
	}
	if c.Conn != nil {
		msg := websocket.FormatCloseMessage(code, reason)
		_ = c.Conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(time.Second))
		_ = c.Conn.Close()
	}
}

func sendRaw(conn *websocket.Conn, t string, payload any) {
	if conn != nil {
		_ = conn.WriteJSON(map[string]any{"type": t, "payload": payload})
	}
}

func decodeMessage(raw []byte) (string, map[string]any, bool) {
	var msg struct {
		Type    string         `json:"type"`
		Payload map[string]any `json:"payload"`
	}
	if json.Unmarshal(raw, &msg) != nil || msg.Type == "" {
		return "", nil, false
	}
	if msg.Payload == nil {
		msg.Payload = map[string]any{}
	}
	return msg.Type, msg.Payload, true
}
