package queue

import "context"

func (s *Service) broadcastQueueUpdate(ctx context.Context, name string) error {
	items, err := s.cache.Queue(ctx, name)
	if err != nil {
		return err
	}
	total := len(items)
	positions := map[string]int{}
	for i, item := range items {
		positions[item.SocketID] = i + 1
	}
	for _, client := range s.hub.Clients(name) {
		client.Send("updateQueue", map[string]any{
			"yourPosition": positions[client.ID],
			"totalWaiting": total,
		})
	}
	return nil
}
