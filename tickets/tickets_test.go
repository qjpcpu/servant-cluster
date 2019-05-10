package tickets

import (
	"testing"
)

func TestTQ(t *testing.T) {
	q := NewQueue()
	q.Set(nil)
	list1 := q.Get()
	for _, tk := range list1 {
		t.Log(tk.ID)
	}
	list := Tickets{
		Ticket{
			ID: "1",
		},
		Ticket{
			ID: "2",
		},
	}
	q.Set(list)
	list1 = q.Get()
	for _, tk := range list1 {
		t.Log(tk.ID)
	}
}
