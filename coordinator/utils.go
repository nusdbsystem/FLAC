package coordinator

import (
	"FLAC/participant"
	"time"
)

func RemoteTestkit(caID string) *Context {
	stmt := &Context{}
	var Arg = []string{"*", "*", caID}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch
	return stmt
}

func BatchTestKit() (*Context, []*participant.Context) {
	stmt := &Context{}
	caID := "127.0.0.1:5001"
	var Arg = []string{"*", "*", caID}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch

	return stmt, participant.OUParticipantsTestKitBatch()
}

func TpcTestKit() (*Context, []*participant.Context) {
	stmt := &Context{}
	caID := "127.0.0.1:5001"
	var Arg = []string{"*", "*", caID}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch
	time.Sleep(10 * time.Millisecond)

	return stmt, participant.OUParticipantsTestKitTPC()
}
