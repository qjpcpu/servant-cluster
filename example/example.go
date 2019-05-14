package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/qjpcpu/servant-cluster/fsn"
	"github.com/qjpcpu/servant-cluster/master"
	"github.com/qjpcpu/servant-cluster/tickets"
)

var (
	allTickets tickets.Tickets
	f          *fsn.Grail
)

func main() {
	loadTicketsFromStorage()
	f = &fsn.Grail{
		EtcdEndpoints:           []string{"127.0.0.1:2379"},
		DispatchHandler:         masterDispatchHandler,
		ServantHandler:          servantHandler,
		SysFetcher:              sysFetcher, // optional
		MaxServantInProccess:    2,
		IP:                      "127.0.0.1",
		EtcdPrefix:              "/servant-cluster/example",
		MasterScheduleInterval:  60 * time.Second,
		ServantScheduleInterval: 2 * time.Second,
		//LogFile:                 "./log/example.log",
	}
	if err := f.Boot(); err != nil {
		fmt.Println("fsn boot fail:", err)
		return
	}
	stopC := make(chan struct{}, 1)
	<-stopC
}

func masterDispatchHandler(lastDisptch *master.CurrentDispatch, newDispatch *master.NewDispatch) error {
	fmt.Println("==================current dispatch==================")
	for _, s := range lastDisptch.ServantPayloads {
		fmt.Printf("servant %v tickets: %s system info:%s\n", s.ServantID, s.Tickets.Summary(), string(s.SystemStats))
	}
	fmt.Println("==================current dispatch==================")

	master.ConservativeAverageDispatch(allTickets, lastDisptch, newDispatch)

	fmt.Println("==================new dispatch==================")
	for _, s := range newDispatch.ServantPayloads {
		fmt.Printf("servant %v tickets: %s\n", s.ServantID, s.Tickets.Summary())
	}
	fmt.Println("==================new dispatch==================")
	return nil
}

func servantHandler(t tickets.Ticket) error {
	fmt.Printf("consume ticket id:%s data:%s\n", t.ID, string(t.Content))
	return nil
}

func sysFetcher() ([]byte, error) {
	info := fmt.Sprintf("my addr: %s, time: %s", f.Addr(), time.Now().Format("2006-01-02 15:04:05"))
	return []byte(info), nil
}

func loadTicketsFromStorage() {
	text := `[{"ID":"1","Content":"ZTM2NTc2NzcxNjU2MDBjMTdiYmE1YmY2MDc5ZDdjNzA=","Type":0},{"ID":"2","Content":"NWNmZTkzOWQ2ODEwODUwMWIxYWQ3Y2Y1NGJlNWE5OTU=","Type":0},{"ID":"3","Content":"Nzk1ODNlYjIxZWQxZTNhZjhmZTkwNzBkNjVmYzZlZWQ=","Type":0},{"ID":"4","Content":"YzlmZjY2MGNhNTQ5MjRkNzBjZWE4Y2I1OWE3OTRiNjc=","Type":0},{"ID":"5","Content":"MTUxMTZkZjNkZGQyMjY4NzE2ODQ3MWI5ODdkMDE2ODc=","Type":0}]`
	json.Unmarshal([]byte(text), &allTickets)
	for i := range allTickets {
		allTickets[i].Type = tickets.SolidTicket
	}
}
