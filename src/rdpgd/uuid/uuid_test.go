package uuid

import "testing"

func TestUUID(t *testing.T) {
	ua := NewUUID()
	ub := NewUUID()
	if len(ua) != 16 {
		t.Errorf("Expecting UUID len of 16, got %d\n", len(ua))
	}
	if len(ua.String()) != 36 {
		t.Errorf("Expecting UUID hex string len of 36, got %d\n", len(ua.String()))
	}
	if ua == ub {
		t.Errorf("Expecting different UUIDs to be different, but they are the same.\n")
	}
}
