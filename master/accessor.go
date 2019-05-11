package master

import (
	"sort"
	"strings"

	"github.com/qjpcpu/servant-cluster/tickets"
)

type ServantPayload struct {
	ServantID   string
	Tickets     tickets.Tickets
	SystemStats []byte
}

type ServantPayloads []ServantPayload
type ServantPayloadsByTickets []ServantPayload

func (a ServantPayloads) Len() int           { return len(a) }
func (a ServantPayloads) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ServantPayloads) Less(i, j int) bool { return a[i].ServantID < a[j].ServantID }

func (a ServantPayloadsByTickets) Len() int           { return len(a) }
func (a ServantPayloadsByTickets) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ServantPayloadsByTickets) Less(i, j int) bool { return len(a[i].Tickets) < len(a[j].Tickets) }

type CurrentDispatch struct {
	ServantPayloads ServantPayloads
}

type NewDispatch struct {
	ForceFlush      bool
	ServantPayloads ServantPayloads
}

func (sp ServantPayloads) Equals(sp1 ServantPayloads) bool {
	var equlas bool
	for doOnce := true; doOnce; doOnce = false {
		if len(sp) != len(sp1) {
			break
		}
		map1 := make(map[string]string)
		for _, s := range sp {
			var ids []string
			for _, t := range s.Tickets {
				ids = append(ids, t.ID)
			}
			sort.Strings(ids)
			map1[s.ServantID] = strings.Join(ids, ",")
		}
		for _, s := range sp1 {
			if _, ok := map1[s.ServantID]; !ok {
				return false
			}
			var ids []string
			for _, t := range s.Tickets {
				ids = append(ids, t.ID)
			}
			sort.Strings(ids)
			if map1[s.ServantID] != strings.Join(ids, ",") {
				return false
			}
		}
		equlas = true
	}
	return equlas
}

type DispatchHandler func(*CurrentDispatch, *NewDispatch) error
