package master

import (
	"github.com/qjpcpu/servant-cluster/tickets"
	"github.com/qjpcpu/servant-cluster/util"
	"sort"
)

func ConservativeAverageDispatch(tks tickets.Tickets, last *CurrentDispatch, newDis *NewDispatch) error {
	ticketCount, servantCount := len(tks), len(last.ServantPayloads)
	if servantCount == 0 {
		return nil
	}
	average := ticketCount / servantCount
	if average == 0 {
		average = 1
	}
	ticketMap := make(map[string]tickets.Ticket)
	for _, tk := range tks {
		ticketMap[tk.ID] = tk
	}
	var newPayloads ServantPayloadsByTickets
	for _, lastp := range last.ServantPayloads {
		// remove invalid tickets
		var delCnt int
		for i := 0; i < len(lastp.Tickets); i++ {
			if nt, ok := ticketMap[lastp.Tickets[i].ID]; !ok {
				delCnt++
			} else {
				lastp.Tickets[i-delCnt] = nt
			}
		}
		lastp.Tickets = lastp.Tickets[:len(lastp.Tickets)-delCnt]
		if len(lastp.Tickets) > average {
			newPayloads = append(newPayloads, ServantPayload{
				ServantID: lastp.ServantID,
				Tickets:   lastp.Tickets[:average],
			})
		} else {
			newPayloads = append(newPayloads, lastp)
		}
		// swipe dispatched tickets
		for i := 0; i < len(lastp.Tickets) && i < average; i++ {
			delete(ticketMap, lastp.Tickets[i].ID)
		}
	}
	sort.Sort(newPayloads)
	var remainTickets tickets.Tickets
	for _, tk := range ticketMap {
		remainTickets = append(remainTickets, tk)
	}
	sort.Sort(remainTickets)
	remainCount := len(remainTickets)
	for i := len(newPayloads) - 1; i >= 0 && remainCount > 0; i-- {
		diff := util.Min(average-len(newPayloads[i].Tickets), remainCount)
		if diff > 0 {
			newPayloads[i].Tickets = append(newPayloads[i].Tickets, remainTickets[remainCount-diff:remainCount]...)
			remainCount -= diff
		}
	}
	if len(newPayloads) > 0 {
		var j int
		for i := 0; i < remainCount; i++ {
			newPayloads[j%len(newPayloads)].Tickets = append(newPayloads[j%len(newPayloads)].Tickets, remainTickets[i])
			j++
		}
	}
	newDis.ServantPayloads = (ServantPayloads)(newPayloads)
	return nil
}
