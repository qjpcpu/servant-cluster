package servant

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qjpcpu/log"
	"github.com/qjpcpu/servant-cluster/tickets"
	"github.com/qjpcpu/servant-cluster/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

type ServantPool struct {
	mutex                  *sync.Mutex
	interval               time.Duration
	maxServant             int
	active                 []*srvt
	silent                 []*srvt
	idInc                  int32
	closeC                 chan struct{}
	requestMasterScheduleC chan struct{}
	jobHandler             ServantHandler
	tq                     *tickets.Queue
}

func newPool(q *tickets.Queue, maxW int, workIntervalSec time.Duration, jobHandler ServantHandler) *ServantPool {
	wp := &ServantPool{
		maxServant:             maxW,
		mutex:                  new(sync.Mutex),
		interval:               workIntervalSec,
		closeC:                 make(chan struct{}, 1),
		requestMasterScheduleC: make(chan struct{}),
		jobHandler:             jobHandler,
		tq:                     q,
	}
	go func() {
		for {
			select {
			case <-wp.closeC:
				log.M(util.ModuleName).Info("srvtpool exit.")
				return
			case size := <-q.SizeChangeC():
				wp.ResizeIfNeed(size)
			}
		}
	}()
	return wp
}
func (p *ServantPool) startRegistProcess(cli *clientv3.Client, keyf string, wid string) {
	go func() {
		for {
			p.registProcess(cli, keyf, wid)
			select {
			case <-p.closeC:
				return
			case <-time.After(1 * time.Minute):
			}
		}
	}()
}

func (p *ServantPool) registProcess(cli *clientv3.Client, keyf string, wid string) error {
	session, err := concurrency.NewSession(cli, concurrency.WithTTL(10))
	if err != nil {
		return err
	}
	defer session.Close()
	k := fmt.Sprintf("%s/%x/%s", util.ServantKey(keyf), session.Lease(), wid)
	log.M(util.ModuleName).Debugf("regist self to %s", k)
	client := session.Client()
	if _, err = client.Put(context.Background(), k, wid, clientv3.WithLease(session.Lease())); err != nil {
		return err
	}
HOLDPROCESS:
	for {
		select {
		case <-p.requestMasterScheduleC:
			client.Put(context.Background(), k, wid, clientv3.WithLease(session.Lease()))
			log.M(util.ModuleName).Debugf("%s request master reschedule", k)
		case <-p.closeC:
			client.Delete(context.Background(), k, clientv3.WithLease(session.Lease()))
			log.M(util.ModuleName).Debugf("deregist self from %s", k)
			break HOLDPROCESS
		case <-session.Done():
			break HOLDPROCESS
		}
	}
	return nil
}

func (p *ServantPool) ResizeIfNeed(ticketCount int) {
	wc := p.ServantCount()
	if ticketCount < wc {
		p.RemoveServant(wc - ticketCount)
		return
	}
	if ticketCount > wc && p.maxServant > wc {
		p.AddServant(util.Min(p.maxServant, ticketCount) - wc)
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
	log.M(util.ModuleName).Infof("increase worker up to %d", len(p.active))
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
	log.M(util.ModuleName).Infof("decrease worker down to %d", len(p.active))
}

func (p *ServantPool) ServantCount() int {
	return len(p.active)
}

func (p *ServantPool) RequestMasterReschedule() {
	select {
	case p.requestMasterScheduleC <- struct{}{}:
	default:
	}
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
