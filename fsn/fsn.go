package fsn

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/qjpcpu/log"
	"github.com/qjpcpu/servant-cluster/master"
	"github.com/qjpcpu/servant-cluster/proto"
	"github.com/qjpcpu/servant-cluster/servant"
	"github.com/qjpcpu/servant-cluster/tickets"
	"github.com/qjpcpu/servant-cluster/util"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

type Fsn struct {
	// config fields
	// etcd endpoints
	EtcdEndpoints []string
	// dispatch implements of master
	DispatchHandler master.DispatchHandler
	// ticket servant handler of servant
	ServantHandler servant.ServantHandler
	// report servant current system info
	SysFetcher tickets.SysInfoGetter
	// max servant parallel in proccess
	MaxServantInProccess int
	// ip of current host
	IP string
	// etcd key prefix for ha and servants cluster
	EtcdPrefix string
	// master schedule interval
	MasterScheduleInterval time.Duration
	// servant worker schedule interval for
	ServantScheduleInterval time.Duration
	// fsn log file
	LogFile string
	Debug   bool
	// generated in runtime
	stopped     int32
	tq          *tickets.Queue
	etcdCli     *clientv3.Client
	masterCtrl  *master.Master
	servantPool *servant.ServantPool
	grpcServer  *grpc.Server
	port        string
}

func (f *Fsn) Boot() error {
	if f.stopped == 1 {
		return errors.New("already stopped")
	}
	// setup log
	f.setupLog()

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

func (f *Fsn) Addr() string {
	return f.IP + f.port
}

func (f *Fsn) setupLog() {
	var logFmt, logLevel string
	if f.Debug {
		logFmt = "\033[1;33m%{level}\033[0m \033[1;36m%{time:2006-01-02 15:04:05.000}\033[0m \033[0;32m[servant-cluster]\033[0m \033[0;34m%{shortfile}\033[0m %{message}"
		logLevel = "debug"
	} else {
		logFmt = "\033[1;36m%{time:2006-01-02 15:04:05.000}\033[0m \033[0;32m[servant-cluster]\033[0m %{message}"
		logLevel = "info"
	}
	log.GetMBuilder(util.ModuleName).SetFormat(logFmt).SetLevel(logLevel).SetFile(f.LogFile).Submit()
}

func (f *Fsn) startServant() error {
	if f.ServantHandler == nil {
		return errors.New("no ServantHandler found")
	}
	if f.EtcdPrefix == "" {
		return errors.New("bad etcd EtcdPrefix key")
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
	sb.SetKeyPrefix(f.EtcdPrefix)
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
	if f.EtcdPrefix == "" {
		return errors.New("bad etcd EtcdPrefix key")
	}
	f.masterCtrl = &master.Master{
		HaEtcdEndpoints:  f.EtcdEndpoints,
		Prefix:           f.EtcdPrefix,
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
	server := servant.NewTicketInfoServer(f.tq, f.SysFetcher)
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
	f.grpcServer = grpcServer
	return server, nil
}

func (f *Fsn) Shutdown() {
	if atomic.CompareAndSwapInt32(&f.stopped, 0, 1) {
		// stop master
		f.masterCtrl.Stop()
		// stop servants
		f.servantPool.Stop()
		// stop grpc server
		f.grpcServer.Stop()
		// close etcd client
		f.etcdCli.Close()
	}
}
