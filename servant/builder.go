package servant

import (
	"context"
	"fmt"
	"time"

	"github.com/qjpcpu/log"
	"github.com/qjpcpu/servant-cluster/tickets"
	"github.com/qjpcpu/servant-cluster/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

type ServantBuilder struct {
	cli         *clientv3.Client
	keyPrefix   string
	wid         string
	workerNum   int
	intervalSec time.Duration
	jobHandler  ServantHandler
	tq          *tickets.Queue
}

func Builder() *ServantBuilder {
	return &ServantBuilder{}
}

func (wb *ServantBuilder) SetTicketsQueue(q *tickets.Queue) *ServantBuilder {
	wb.tq = q
	return wb
}
func (wb *ServantBuilder) SetEtcdCli(cli *clientv3.Client) *ServantBuilder {
	wb.cli = cli
	return wb
}

func (wb *ServantBuilder) SetServantHandler(sh ServantHandler) *ServantBuilder {
	wb.jobHandler = sh
	return wb
}
func (wb *ServantBuilder) SetKeyPrefix(keyPrefix string) *ServantBuilder {
	wb.keyPrefix = keyPrefix
	return wb
}

// SetServantID should be ip:port
func (wb *ServantBuilder) SetServantID(wid string) *ServantBuilder {
	wb.wid = wid
	return wb
}
func (wb *ServantBuilder) SetServantMaxNum(workerNum int) *ServantBuilder {
	wb.workerNum = workerNum
	return wb
}
func (wb *ServantBuilder) SetInterval(intervalSec time.Duration) *ServantBuilder {
	if intervalSec == 0 {
		intervalSec = 1 * time.Millisecond
	}
	wb.intervalSec = intervalSec
	return wb
}
func (wb *ServantBuilder) Run() *ServantPool {
	if wb.workerNum == 0 {
		wb.workerNum = 1
	}
	if wb.intervalSec == 0 {
		wb.intervalSec = 5
	}
	sp := newPool(wb.tq, wb.workerNum, wb.intervalSec, wb.jobHandler)
	go func(closeC <-chan struct{}) {
		for {
			registProcess(wb.cli, wb.keyPrefix, wb.wid, closeC)
			select {
			case <-closeC:
				return
			default:
			}
		}
	}(sp.closeC)
	return sp
}

func registProcess(cli *clientv3.Client, keyf string, wid string, closeC <-chan struct{}) error {
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
	select {
	case <-closeC:
	case <-session.Done():
	}
	return nil
}
