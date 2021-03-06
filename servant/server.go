package servant

import (
	"context"

	"github.com/qjpcpu/servant-cluster/proto"
	"github.com/qjpcpu/servant-cluster/tickets"
)

type TicketInfoServer struct {
	Addr    string
	tq      *tickets.Queue
	sysFunc tickets.SysInfoGetter
}

func NewTicketInfoServer(tq *tickets.Queue, sysGetter tickets.SysInfoGetter) *TicketInfoServer {
	ts := &TicketInfoServer{tq: tq, sysFunc: sysGetter}
	return ts
}

func (s *TicketInfoServer) GetTickets(c context.Context, e *proto.Empty) (*proto.TicketsInfo, error) {
	ts := s.tq.Get()
	ti := &proto.TicketsInfo{}
	for _, t := range ts {
		pt := &proto.TicketInfo{
			Id:      t.ID,
			Type:    int32(t.Type),
			Content: t.Content,
		}
		ti.TicketsInfo = append(ti.TicketsInfo, pt)
	}
	if s.sysFunc != nil {
		stats, err := s.sysFunc()
		if err != nil {
			return nil, err
		}
		ti.SysInfo = &proto.SystemInfo{Stats: stats}
	}
	return ti, nil
}

func (s *TicketInfoServer) SetTickets(c context.Context, info *proto.TicketsInfo) (*proto.Empty, error) {
	var ts tickets.Tickets
	for _, t := range info.TicketsInfo {
		ts = append(ts, tickets.Ticket{
			ID:      t.Id,
			Type:    tickets.TicketType(t.Type),
			Content: t.Content,
		})
	}
	err := s.tq.Set(ts)
	return &proto.Empty{}, err
}
