package queue

import "testing"

func TestDecodeMessage(t *testing.T) {
	tpe, payload, ok := decodeMessage([]byte(`{"type":"joinQueue","payload":{"eventId":"e"}}`))
	if !ok || tpe != "joinQueue" || payload["eventId"] != "e" {
		t.Fatalf("unexpected decoded message: %s %#v %t", tpe, payload, ok)
	}
	if _, _, ok := decodeMessage([]byte(`bad`)); ok {
		t.Fatal("expected invalid message")
	}
}

func TestQueueNames(t *testing.T) {
	name := queueName("event", "date")
	eventID, dateID, ok := splitQueueName(name)
	if name != "queue:event_date" || !ok || eventID != "event" || dateID != "date" {
		t.Fatalf("unexpected queue name: %s %s %s %t", name, eventID, dateID, ok)
	}
	if _, _, ok := splitQueueName("queue:bad"); ok {
		t.Fatal("expected invalid queue name")
	}
}

func TestServiceURL(t *testing.T) {
	got := serviceURL("http://svc", "scheduling/queue/status")
	if got != "http://svc/scheduling/queue/status" {
		t.Fatalf("unexpected service url: %s", got)
	}
}
