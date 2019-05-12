package master

import (
	"context"
	"strings"

	"github.com/qjpcpu/log"
	"github.com/qjpcpu/servant-cluster/proto"
	"github.com/qjpcpu/servant-cluster/tickets"
	"github.com/qjpcpu/servant-cluster/util"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

type servantAccessor struct {
	cli *clientv3.Client
	key string
}

func newServantAccessor(cli *clientv3.Client, key string) *servantAccessor {
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}
	return &servantAccessor{
		cli: cli,
		key: key,
	}
}

func (wa *servantAccessor) watch(notifyC chan<- struct{}, closeC <-chan struct{}) error {
	wchan := wa.cli.Watch(context.Background(), wa.key, clientv3.WithPrefix())
	go func() {
		for {
			select {
			case <-closeC:
				log.M(util.ModuleName).Debug("servant watch is closed.")
				return
			case wr := <-wchan:
				if wr.Canceled {
					log.M(util.ModuleName).Debug("servant watch is canceled.")
					return
				} else if wr.Created {
					log.M(util.ModuleName).Debug("servant watch is created.")
				} else {
					log.M(util.ModuleName).Debug("servants cluster changed.")
					select {
					case notifyC <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	return nil
}

func (wa *servantAccessor) GetServants() ([]string, error) {
	resp, err := wa.cli.Get(context.Background(), wa.key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		log.M(util.ModuleName).Debug("no servants ready.")
		return nil, nil
	}
	var list []string
	for _, kv := range resp.Kvs {
		tokens := strings.Split(string(kv.Key), "/")
		list = append(list, tokens[len(tokens)-1])
	}
	log.M(util.ModuleName).Debugf("get servants:%v", list)
	return list, nil
}

func (wa *servantAccessor) GetServantTickets(wid string) (tickets.Tickets, []byte, error) {
	conn, err := grpc.Dial(wid, grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	client := proto.NewTicketDispatcherClient(conn)
	info, err := client.GetTickets(context.Background(), &proto.Empty{})
	if err != nil {
		log.M(util.ModuleName).Errorf("get servant tickets fail:%v", err)
		return nil, nil, err
	}
	var tks tickets.Tickets
	for _, tk := range info.TicketsInfo {
		tks = append(tks, tickets.Ticket{
			ID:      tk.Id,
			Content: tk.Content,
			Type:    tickets.TicketType(tk.Type),
		})
	}
	var stats []byte
	if sys := info.GetSysInfo(); sys != nil {
		stats = sys.GetStats()
	}
	return tks, stats, nil
}

func (wa *servantAccessor) SetServantTickets(wid string, tks tickets.Tickets) error {
	conn, err := grpc.Dial(wid, grpc.WithInsecure())
	if err != nil {
		log.M(util.ModuleName).Errorf("set servant tickets fail:%v", err)
		return err
	}
	defer conn.Close()
	client := proto.NewTicketDispatcherClient(conn)
	ti := &proto.TicketsInfo{}
	for _, tk := range tks {
		ti.TicketsInfo = append(ti.TicketsInfo, &proto.TicketInfo{
			Id:      tk.ID,
			Content: tk.Content,
			Type:    int32(tk.Type),
		})
	}
	_, err = client.SetTickets(context.Background(), ti)
	return err
}
