package servant

import (
	"sync"
	"time"

	"github.com/qjpcpu/log"
	"github.com/qjpcpu/servant-cluster/tickets"
	"github.com/qjpcpu/servant-cluster/util"
)

type srvt struct {
	id       int32
	handler  ServantHandler
	stopC    chan struct{}
	silentC  chan struct{}
	activeC  chan struct{}
	interval time.Duration
	tq       *tickets.Queue
}

func newServant(id int32, tq *tickets.Queue, intervalSec time.Duration, handler ServantHandler) *srvt {
	return &srvt{
		id:       id,
		stopC:    make(chan struct{}, 1),
		silentC:  make(chan struct{}, 1),
		handler:  handler,
		activeC:  make(chan struct{}, 1),
		interval: intervalSec,
		tq:       tq,
	}
}

func (w *srvt) start(wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
		log.M(util.ModuleName).Infof("worker-%d exit.", w.id)
	}()
	for {
		select {
		case <-w.stopC:
			return
		case t := <-w.tq.RequestC():
			w.doWork(t)

		case <-w.silentC:
			select {
			case <-w.activeC:
			case <-w.stopC:
				return
			}
		}
	}
}
func (w *srvt) doSafeWork(t tickets.Ticket) {
	defer w.tq.Recycle(t)
	if err := w.handler(t); err != nil {
		log.M(util.ModuleName).Debugf("[worker-%d] dowork fail:%v", w.id, err)
	}
}

func (w *srvt) doWork(t tickets.Ticket) {
	w.doSafeWork(t)
	select {
	case <-w.silentC:
		log.M(util.ModuleName).Infof("[worker-%d] goes silent", w.id)
		select {
		case <-w.activeC:
			return
		case <-w.stopC:
			return
		}
	case <-w.stopC:
		return
	case <-time.After(w.interval):
	}
}

func (w *srvt) goSilent() {
	w.silentC <- struct{}{}
}

func (w *srvt) goActive() {
	w.activeC <- struct{}{}
}

func (w *srvt) stop() {
	close(w.stopC)
}

type ServantHandler func(tickets.Ticket) error
