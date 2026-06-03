package queue

import (
	"net/url"
	"strings"
)

func str(p map[string]any, key string) string {
	if value, ok := p[key].(string); ok {
		return value
	}
	return ""
}

func roomName(eventID, eventDateID string) string {
	return eventID + "_" + eventDateID
}

func queueName(eventID, eventDateID string) string {
	return "queue:" + roomName(eventID, eventDateID)
}

func splitQueueName(name string) (string, string, bool) {
	raw := strings.TrimPrefix(name, "queue:")
	parts := strings.Split(raw, "_")
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func serviceURL(base, path string) string {
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	u, err := url.JoinPath(base, path)
	if err != nil {
		return base + path
	}
	return u
}
