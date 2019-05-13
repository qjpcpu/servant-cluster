package servant

import (
	"time"

	"github.com/qjpcpu/servant-cluster/tickets"
	"go.etcd.io/etcd/clientv3"
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
	sp.startRegistProcess(wb.cli, wb.keyPrefix, wb.wid)
	return sp
}
