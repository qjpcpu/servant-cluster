package servant

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/qjpcpu/log"
	"github.com/qjpcpu/servant-cluster/tickets"
)

type ServantPool struct {
	mutex      *sync.Mutex
	interval   time.Duration
	maxServant int
	active     []*srvt
	silent     []*srvt
	idInc      int32
	closeC     chan struct{}
	jobHandler ServantHandler
	tq         *tickets.Queue
}

func newPool(q *tickets.Queue, maxW int, workIntervalSec time.Duration, jobHandler ServantHandler) *ServantPool {
	wp := &ServantPool{
		maxServant: maxW,
		mutex:      new(sync.Mutex),
		interval:   workIntervalSec,
		closeC:     make(chan struct{}, 1),
		jobHandler: jobHandler,
		tq:         q,
	}
	go func() {
		for {
			select {
			case <-wp.closeC:
				log.Info("srvtpool exit.")
				return
			case size := <-q.SizeChangeC():
				wp.ResizeIfNeed(size)
			}
		}
	}()
	return wp
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func (p *ServantPool) ResizeIfNeed(ticketCount int) {
	wc := p.ServantCount()
	if ticketCount < wc {
		p.RemoveServant(wc - ticketCount)
		return
	}
	if ticketCount > wc && p.maxServant > wc {
		p.AddServant(min(p.maxServant, ticketCount) - wc)
		return
	}
}

func (p *ServantPool) AddServant(n int) {
	if n <= 0 || len(p.active) == p.maxServant {
		return
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if n+len(p.active) > p.maxServant {
		n = p.maxServant - len(p.active)
	}
	if n > len(p.silent) {
		if len(p.silent) > 0 {
			for i := 0; i < len(p.silent); i++ {
				p.silent[i].goActive()
			}
			p.active = append(p.active, p.silent...)
		}
		n -= len(p.silent)
		for i := 0; i < n; i++ {
			w := newServant(atomic.AddInt32(&p.idInc, 1), p.tq, p.interval, p.jobHandler)
			go w.start()
			p.active = append(p.active, w)
		}
	} else {
		for i := 0; i < n; i++ {
			p.silent[i].goActive()
		}
		p.active = append(p.active, p.silent[0:n]...)
		p.silent = p.silent[n:]
	}
	log.Infof("增加worker至%d个", len(p.active))
}

func (p *ServantPool) RemoveServant(n int) {
	if n <= 0 {
		return
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if n > len(p.active) {
		n = len(p.active)
	}
	j := n
	for i := len(p.active) - 1; i >= 0 && j > 0; i-- {
		p.active[i].goSilent()
		j--
	}
	p.silent = append(p.silent, p.active[len(p.active)-n:]...)
	p.active = p.active[0 : len(p.active)-n]
	log.Infof("减少worker至%d个", len(p.active))
}

func (p *ServantPool) ServantCount() int {
	return len(p.active)
}

func (p *ServantPool) Stop() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for _, w := range p.active {
		w.stop()
	}
	for _, w := range p.silent {
		w.stop()
	}
	close(p.closeC)
	p.active = nil
	p.silent = nil
}
