package fsn

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/qjpcpu/servant-cluster/master"
	"github.com/qjpcpu/servant-cluster/proto"
	"github.com/qjpcpu/servant-cluster/servant"
	"github.com/qjpcpu/servant-cluster/tickets"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

type Fsn struct {
	// config fields
	EtcdEndpoints           []string
	DispatchHandler         master.DispatchHandler
	ServantHandler          servant.ServantHandler
	MaxServantInProccess    int
	IP                      string
	Prefix                  string
	MasterScheduleInterval  time.Duration
	ServantScheduleInterval time.Duration
	// generated in runtime
	tq          *tickets.Queue
	etcdCli     *clientv3.Client
	masterCtrl  *master.Master
	servantPool *servant.ServantPool
	port        string
}

func (f *Fsn) Boot() error {
	// connect etcd
	if err := f.connectEtcd(); err != nil {
		return err
	}
	// create ticket queue
	f.tq = tickets.NewQueue()
	// start grpc server
	tserver, err := f.startServantServer()
	if err != nil {
		return err
	}
	f.port = tserver.Addr
	// start master
	if err = f.startMaster(); err != nil {
		return err
	}
	// start servant
	if err = f.startServant(); err != nil {
		return err
	}
	return nil
}

func (f *Fsn) startServant() error {
	if f.ServantHandler == nil {
		return errors.New("no ServantHandler found")
	}
	if f.Prefix == "" {
		return errors.New("bad etcd prefix key")
	}
	if f.IP == "" {
		return errors.New("no self ip")
	}
	if f.MaxServantInProccess <= 0 {
		return errors.New("one servant is needed at least")
	}
	sb := servant.Builder()
	sb.SetTicketsQueue(f.tq)
	sb.SetEtcdCli(f.etcdCli)
	sb.SetServantHandler(f.ServantHandler)
	sb.SetKeyPrefix(f.Prefix)
	sb.SetServantID(f.IP + f.port)
	sb.SetServantMaxNum(f.MaxServantInProccess)
	sb.SetInterval(f.ServantScheduleInterval)
	f.servantPool = sb.Run()
	return nil
}

func (f *Fsn) startMaster() error {
	if f.DispatchHandler == nil {
		return errors.New("no DispatchHandler found")
	}
	if f.Prefix == "" {
		return errors.New("bad etcd prefix key")
	}
	f.masterCtrl = &master.Master{
		HaEtcdEndpoints:  f.EtcdEndpoints,
		Prefix:           f.Prefix,
		ScheduleInterval: f.MasterScheduleInterval,
		DispatchHandler:  f.DispatchHandler,
		EtcdCli:          f.etcdCli,
	}
	go f.masterCtrl.Run()
	return nil
}

func (f *Fsn) connectEtcd() error {
	if len(f.EtcdEndpoints) == 0 {
		return errors.New("bad etcd config")
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: f.EtcdEndpoints})
	if err != nil {
		return err
	}
	f.etcdCli = cli
	return nil
}

func (f *Fsn) startServantServer() (*servant.TicketInfoServer, error) {
	server := servant.NewTicketInfoServer(f.tq)
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	server.Addr = fmt.Sprintf(":%d", port)
	grpcServer := grpc.NewServer()
	proto.RegisterTicketDispatcherServer(grpcServer, server)
	fmt.Printf("Listening and serving grpc on %s\n", server.Addr)
	go grpcServer.Serve(ln)
	return server, nil
}