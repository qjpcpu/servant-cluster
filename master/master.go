package master

import (
	"errors"
	"time"

	"github.com/qjpcpu/common/election"
	"github.com/qjpcpu/log"
	"github.com/qjpcpu/servant-cluster/tickets"
	"github.com/qjpcpu/servant-cluster/util"
	"go.etcd.io/etcd/clientv3"
)

type Master struct {
	HaEtcdEndpoints  []string
	Prefix           string
	ScheduleInterval time.Duration
	DispatchHandler  DispatchHandler
	EtcdCli          *clientv3.Client

	ha     *election.HA
	sa     *servantAccessor
	closeC chan struct{}
}

func (m *Master) Run() error {
	if m.DispatchHandler == nil || m.EtcdCli == nil || len(m.HaEtcdEndpoints) == 0 {
		return errors.New("bad master config")
	}
	m.closeC = make(chan struct{})
	ha := election.New(m.HaEtcdEndpoints, util.MasterKey(m.Prefix)).TTL(15)
	m.ha = ha
	m.sa = newServantAccessor(m.EtcdCli, util.ServantKey(m.Prefix))
	go ha.Start()
	if !ha.IsLeader() {
		log.M(util.ModuleName).Info("wait to be dispatcher")
		for {
			role := <-ha.RoleC()
			if role == election.Leader {
				break
			}
		}
	}
	if m.ScheduleInterval == 0 {
		m.ScheduleInterval = 1 * time.Minute
	}
	log.M(util.ModuleName).Info("I am dispatcher now.")
	for {
		if err := m.loopOnce(); err != nil {
			log.M(util.ModuleName).Errorf("dispatch fail:%v", err)
		}
		select {
		case role := <-ha.RoleC():
			if role == election.Leader {
				log.M(util.ModuleName).Info("I am dispatcher now, restart dispatching")
			} else {
				log.M(util.ModuleName).Info("Switch to candidate, pause dispatching")
				for {
					role2 := <-ha.RoleC()
					if role2 == election.Leader {
						break
					}
				}
			}
		case <-time.After(m.ScheduleInterval):
		case <-m.closeC:
			return nil
		}
	}
}

func (m *Master) Stop() {
	close(m.closeC)
	m.ha.Stop()
}

func (m *Master) loopOnce() error {
	servantList, err := m.sa.GetServants()
	if err != nil {
		log.M(util.ModuleName).Errorf("get servants fail:%v", err)
		return err
	}
	servantTicketsM := make(map[string]tickets.Tickets)
	var old ServantPayloads
	for _, srvt := range servantList {
		tks, err := m.sa.GetServantTickets(srvt)
		if err != nil {
			log.M(util.ModuleName).Errorf("get servant %s tickets fail:%v", srvt, err)
			return err
		}
		servantTicketsM[srvt] = tks
		old = append(old, ServantPayload{
			ServantID: srvt,
			Tickets:   tks,
		})
	}

	// dispatch
	newDis, err := m.DispatchHandler(old)
	if err != nil {
		log.M(util.ModuleName).Errorf("dispatch fail:%v", err)
		return err
	}
	for _, p := range newDis {
		if ot, ok := servantTicketsM[p.ServantID]; ok && ot.Equals(p.Tickets) {
			log.M(util.ModuleName).Debugf("remain %s %d tickets: %s", p.ServantID, len(p.Tickets), p.Tickets.Summary())
			continue
		}
		if err = m.sa.SetServantTickets(p.ServantID, p.Tickets); err != nil {
			log.M(util.ModuleName).Warningf("dispatch %s tickets fail:%v", p.ServantID, err)
		} else {
			log.M(util.ModuleName).Debugf("dispatch %s %d tickets: %s", p.ServantID, len(p.Tickets), p.Tickets.Summary())
		}
	}
	return nil
}
