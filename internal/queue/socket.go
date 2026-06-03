package queue

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/hjyoon/socketing-queue-server/internal/auth"
)

func (s *Service) HandleHTTP(c *gin.Context) {
	s.ServeWebSocket(c.Writer, c.Request)
}

func (s *Service) ServeWebSocket(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	claims, err := auth.Verify(r.URL.Query().Get("token"), s.cfg.JWTSecret)
	if err != nil {
		sendRaw(conn, "error", map[string]string{"message": "Authentication error"})
		_ = conn.Close()
		return
	}
	sub, _ := claims["sub"].(string)
	client := &Client{
		ID: auth.UUID(), UserID: sub, Claims: claims, Conn: conn,
		Rooms: map[string]bool{},
	}
	s.hub.Add(client)
	s.hub.Join(client, client.ID)
	client.Send("connected", map[string]string{"id": client.ID})
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			s.disconnect(r.Context(), client)
			return
		}
		s.HandleMessage(r.Context(), client, raw)
	}
}
