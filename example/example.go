package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/qjpcpu/servant-cluster/fsn"
	"github.com/qjpcpu/servant-cluster/master"
	"github.com/qjpcpu/servant-cluster/tickets"
)

var (
	allTickets tickets.Tickets
	f          *fsn.Fsn
)

func main() {
	loadTicketsFromStorage()
	f = &fsn.Fsn{
		EtcdEndpoints:           []string{"127.0.0.1:2379"},
		DispatchHandler:         masterDispatchHandler,
		ServantHandler:          servantHandler,
		SysFetcher:              sysFetcher, // optional
		MaxServantInProccess:    2,
		IP:                      "127.0.0.1",
		Prefix:                  "/servant-cluster/example",
		MasterScheduleInterval:  10 * time.Second,
		ServantScheduleInterval: 1 * time.Second,
		//LogFile:                 "./log/example.log",
	}
	if err := f.Boot(); err != nil {
		fmt.Println("fsn boot fail:", err)
		return
	}
	stopC := make(chan struct{}, 1)
	<-stopC
}

func masterDispatchHandler(lastDisptch master.ServantPayloads) (master.ServantPayloads, error) {
	fmt.Println("==================old dispatch==================")
	for _, s := range lastDisptch {
		fmt.Printf("servant %v tickets: %s system info:%s\n", s.ServantID, s.Tickets.Summary(), string(s.SystemStats))
	}
	fmt.Println("==================old dispatch==================")
	// random redispatch
	splitI := rand.Intn(len(allTickets))
	newDispatch := make(master.ServantPayloads, len(lastDisptch))
	for i := 0; i < len(lastDisptch); i++ {
		newDispatch[i].ServantID = lastDisptch[i].ServantID
	}

	for i := 0; i < len(allTickets); i++ {
		si := (i + splitI) % len(newDispatch)
		newDispatch[si].Tickets = append(newDispatch[si].Tickets, allTickets[i])
	}
	fmt.Println("==================new dispatch==================")
	for _, s := range newDispatch {
		fmt.Printf("servant %v tickets: %s\n", s.ServantID, s.Tickets.Summary())
	}
	fmt.Println("==================new dispatch==================")
	return newDispatch, nil
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
}
