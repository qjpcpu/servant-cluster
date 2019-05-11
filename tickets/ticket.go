package tickets

import (
	"sort"
	"strings"
)

type TicketType int32

const (
	OnceTicket TicketType = iota
	SolidTicket
)

type Ticket struct {
	ID       string
	Content  []byte
	Type     TicketType
	revision uint64
}

type Tickets []Ticket

func (a Tickets) Len() int           { return len(a) }
func (a Tickets) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Tickets) Less(i, j int) bool { return a[i].ID < a[j].ID }

func (ts Tickets) Equals(ts1 Tickets) bool {
	if len(ts) != len(ts1) {
		return false
	}
	var list []string
	for _, t := range ts {
		list = append(list, t.ID)
	}
	sort.Strings(list)
	s := strings.Join(list, ",")
	var list1 []string
	for _, t := range ts1 {
		list1 = append(list1, t.ID)
	}
	sort.Strings(list1)
	s1 := strings.Join(list1, ",")
	return s == s1
}

func (ts Tickets) Summary() string {
	var list []string
	for _, t := range ts {
		rs := []rune(t.ID)
		if len(rs) > 11 {
			rs = append(rs[:8], 46, 46, 46)
		}
		list = append(list, string(rs))
	}
	return "[" + strings.Join(list, ",") + "]"
}
