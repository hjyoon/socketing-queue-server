package queue

import "context"

const closePolicyViolation = 1008

func (s *Service) HandleMessage(ctx context.Context, c *Client, raw []byte) {
	t, p, ok := decodeMessage(raw)
	if !ok {
		c.Send("error", map[string]string{"message": "Invalid message format."})
		return
	}
	if t != "joinQueue" {
		c.Send("error", map[string]string{"message": "Unknown message type."})
		return
	}
	if err := s.joinQueue(ctx, c, p); err != nil {
		c.Send("error", map[string]string{"message": "Internal server error."})
		c.Close(1011, "Internal server error")
	}
}

func (s *Service) joinQueue(ctx context.Context, c *Client, p map[string]any) error {
	eventID, dateID := str(p, "eventId"), str(p, "eventDateId")
	if eventID == "" || dateID == "" {
		c.Send("error", map[string]string{"message": "Invalid queue parameters."})
		c.Close(closePolicyViolation, "Invalid queue parameters")
		return nil
	}
	if c.UserID == "" {
		c.Send("error", map[string]string{"message": "Invalid user data."})
		c.Close(closePolicyViolation, "Invalid user data")
		return nil
	}
	name := queueName(eventID, dateID)
	item := QueueItem{SocketID: c.ID, UserID: c.UserID}
	index, err := s.cache.FindIndex(ctx, name, item)
	if err != nil {
		return err
	}
	if index != -1 {
		c.Send("error", map[string]string{"message": "Already in the queue."})
		c.Close(closePolicyViolation, "Already in the queue")
		return nil
	}
	if err := s.cache.AddClient(ctx, name, item); err != nil {
		return err
	}
	s.hub.Join(c, name)
	if err := s.cache.AddStream(ctx, eventID, dateID); err != nil {
		return err
	}
	return s.notifyScheduling(ctx, eventID, dateID)
}

func (s *Service) disconnect(ctx context.Context, c *Client) {
	s.hub.Remove(c)
	if c.UserID == "" {
		return
	}
	queues, err := s.cache.ScanQueues(ctx)
	if err != nil {
		return
	}
	for _, name := range queues {
		n, err := s.cache.RemoveClient(ctx, name, QueueItem{SocketID: c.ID, UserID: c.UserID})
		if err == nil && n > 0 {
			if eventID, dateID, ok := splitQueueName(name); ok {
				_ = s.cache.AddStream(ctx, eventID, dateID)
			}
			return
		}
	}
}
