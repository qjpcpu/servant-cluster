package tickets

import (
	"errors"
	"sync/atomic"
	"unsafe"

	"github.com/qjpcpu/common/joint"
)

func NewQueue() *Queue {
	ticketq := &Queue{
		in:    make(chan Ticket),
		out:   make(chan Ticket),
		sizeC: make(chan int, 1),
	}
	pipe, _ := joint.Pipe(ticketq.in, ticketq.out)
	pipe.SetFilter(func(tk interface{}) bool {
		t := tk.(Ticket)
		return t.revision >= atomic.LoadUint64(&ticketq.minRevision)
	})
	return ticketq
}

type Queue struct {
	minRevision uint64
	ticketList  Tickets
	in, out     chan Ticket
	sizeC       chan int
}

func (ticketq *Queue) Set(list Tickets) error {
	ptr := atomic.LoadPointer((*unsafe.Pointer)((unsafe.Pointer)(&ticketq.ticketList)))
	if !atomic.CompareAndSwapPointer((*unsafe.Pointer)((unsafe.Pointer)(&ticketq.ticketList)), ptr, unsafe.Pointer(&list)) {
		return errors.New("set tickets fail")
	}
	minRevision := atomic.AddUint64(&ticketq.minRevision, 1)
	for i := range list {
		list[i].revision = minRevision
		ticketq.in <- list[i]
	}

	select {
	case ticketq.sizeC <- len(list):
	default:
	}
	return nil
}

func (ticketq *Queue) Get() Tickets {
	ptr := atomic.LoadPointer((*unsafe.Pointer)((unsafe.Pointer)(&ticketq.ticketList)))
	if ptr == nil {
		return nil
	}
	return *((*Tickets)(ptr))
}

func (ticketq *Queue) SizeChangeC() <-chan int {
	return ticketq.sizeC
}

func (ticketq *Queue) RequestC() <-chan Ticket {
	return ticketq.out
}

func (ticketq *Queue) Recycle(t Ticket) {
	if t.Type == OnceTicket {
		return
	}
	if t.revision < atomic.LoadUint64(&ticketq.minRevision) {
		return
	}
	ticketq.in <- t
}
