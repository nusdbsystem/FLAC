package participant

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"testing"
	"time"
)

var address = []string{"127.0.0.1:6001", "127.0.0.1:6002", "127.0.0.1:6003"}

func makeLocal() {
	configs.SetLocal()
	configs.BasicWaitTime = 20
	configs.SetR(1)
	configs.SetMsgDelay4FLAC(1)
	configs.SetConcurrency(1)
	configs.SetServerTimeOut(28)
	configs.SetDown(0)
	configs.SetNF(-1)
	configs.SetMinLevel(1)
}

func ParticipantsTestKit() []*Context {
	stmts := make([]*Context, 3)
	ch := make(chan bool)

	var Args1 = []string{"*", "*", address[0], "5"}
	stmts[0] = &Context{}
	go begin(stmts[0], ch, Args1[2])
	<-ch

	var Args2 = []string{"*", "*", address[1], "5"}
	stmts[1] = &Context{}
	go begin(stmts[1], ch, Args2[2])
	<-ch

	var Args3 = []string{"*", "*", address[2], "5"}
	stmts[2] = &Context{}
	go begin(stmts[2], ch, Args3[2])
	<-ch

	for i := 0; i < 3; i++ {
		//fmt.Printf("the address is %v\n", stmts[i].Manager.stmt.address)
		//utils.JPrint(stmts[i].Manager.Kv)
		//utils.JPrint(stmts[i].Manager.Kv[stmts[i].Manager.stmt.address])
		stmts[i].Manager.Kv[stmts[i].Manager.stmt.address].SetDDL(time.Millisecond * 200)
		for j := 0; j < 5; j++ {
			stmts[i].Manager.Kv[stmts[i].Manager.stmt.address].Update(uint64(j), j)
		}
	}
	return stmts
}

func StopServers(stmt []*Context) {
	for _, v := range stmt {
		v.Close()
	}
	//time.Sleep(configs.OptEps * 10)
}

func CheckPreRead(coh *Manager, txn *network.CoordinatorGossip, ans []int, shardID string) {
	if len(txn.OptList) > 0 {
		txn.OptList = txn.OptList[:0]
	}
	for i := 0; i < 5; i++ {
		txn.AppendRead(shardID, uint64(i))
	}
	ok, val := coh.forTestPreRead(txn)
	//configs.JPrint(ok)
	configs.Assert(ok && len(val) == len(ans), "PreRead Failed or With different length")
	for i := 0; i < len(ans); i++ {
		configs.Assert(ans[i] == val[configs.Hash(coh.stmt.address, uint64(i))], "PreRead Failed with different value")
	}
}

func TestBuildTestCases(t *testing.T) {
	makeLocal()
	stmts := ParticipantsTestKit()
	CheckVal(t, stmts[0].Manager, []int{0, 1, 2, 3, 4})
	CheckVal(t, stmts[1].Manager, []int{0, 1, 2, 3, 4})
	CheckVal(t, stmts[2].Manager, []int{0, 1, 2, 3, 4})
	StopServers(stmts)
}

func TestCohortsPreRead(t *testing.T) {
	makeLocal()
	stmts := ParticipantsTestKit()
	txn1 := network.NewTXPack(1, stmts[0].address, detector.CFNF, address)
	txn2 := network.NewTXPack(2, stmts[1].address, detector.CFNF, address)

	c := make(chan bool)

	go func() {
		CheckPreRead(stmts[1].Manager, txn2, []int{0, 1, 2, 3, 4}, stmts[1].address)
		c <- true
	}()
	go func() {
		CheckPreRead(stmts[0].Manager, txn1, []int{0, 1, 2, 3, 4}, stmts[0].address)
		c <- true
	}()
	<-c
	<-c
	StopServers(stmts)
}

func TestCohortsPreWrite(t *testing.T) {
	makeLocal()
	stmts := ParticipantsTestKit()
	txn1 := network.NewTXPack(1, stmts[0].address, detector.CFNF, address)
	CheckPreRead(stmts[0].Manager, txn1, []int{0, 1, 2, 3, 4}, stmts[0].address)

	txn1.OptList = txn1.OptList[:0]
	for i := 0; i < 5; i++ {
		txn1.AppendUpdate(stmts[0].address, uint64(i), i+1)
	}
	configs.JPrint(txn1)
	ok := stmts[0].Manager.PreWrite(txn1)
	configs.JPrint(ok)
	ok = stmts[0].Manager.Commit(txn1)
	configs.JPrint(ok)
	CheckVal(t, stmts[0].Manager, []int{1, 2, 3, 4, 5})
	StopServers(stmts)
}

func Test2PCCommit(t *testing.T) {
	makeLocal()
	stmts := ParticipantsTestKit()
	txn1 := network.NewTXPack(1, stmts[0].address, detector.CFNF, address)
	CheckPreRead(stmts[0].Manager, txn1, []int{0, 1, 2, 3, 4}, stmts[0].address)
	txn2 := network.NewTXPack(1, stmts[1].address, detector.CFNF, address)
	CheckPreRead(stmts[1].Manager, txn2, []int{0, 1, 2, 3, 4}, stmts[1].address)
	txn1.OptList = txn1.OptList[:0]
	txn2.OptList = txn2.OptList[:0]
	for i := 0; i < 5; i++ {
		txn1.AppendUpdate(stmts[0].address, uint64(i), i+1)
		txn2.AppendUpdate(stmts[1].address, uint64(i), i+2)
	}

	c := make(chan bool)

	go func() {
		ok := stmts[0].Manager.PreWrite(txn1)
		c <- configs.Assert(ok, "PreWrite Made Failed")
	}()
	go func() {
		ok := stmts[1].Manager.PreWrite(txn2)
		c <- configs.Assert(ok, "PreWrite Made Failed")
	}()
	<-c
	<-c

	go func() {
		ok := stmts[0].Manager.Commit(txn1)
		c <- configs.Assert(ok, "Commit Made Failed")
	}()
	go func() {
		ok := stmts[1].Manager.Commit(txn2)
		c <- configs.Assert(ok, "Commit Made Failed")
	}()
	<-c
	<-c

	CheckPreRead(stmts[0].Manager, txn1, []int{1, 2, 3, 4, 5}, stmts[0].address)
	CheckVal(t, stmts[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, stmts[1].Manager, []int{2, 3, 4, 5, 6})
	StopServers(stmts)
}

func Test2PCAbort(t *testing.T) {
	makeLocal()
	stmts := ParticipantsTestKit()
	txn1 := network.NewTXPack(1, stmts[0].address, detector.CFNF, address)
	CheckPreRead(stmts[0].Manager, txn1, []int{0, 1, 2, 3, 4}, stmts[0].address)
	txn2 := network.NewTXPack(1, stmts[1].address, detector.CFNF, address)
	CheckPreRead(stmts[1].Manager, txn2, []int{0, 1, 2, 3, 4}, stmts[1].address)
	txn1.OptList = txn1.OptList[:0]
	txn2.OptList = txn2.OptList[:0]
	for i := 0; i < 5; i++ {
		txn1.AppendUpdate(stmts[0].address, uint64(i), i+1)
		txn2.AppendUpdate(stmts[1].address, uint64(i), i+2)
	}

	ok := stmts[0].Manager.PreWrite(txn1)

	ok = stmts[0].Manager.Abort(txn1)
	ok = ok && stmts[1].Manager.Abort(txn2) // if the first term is false, the commit will not be executed.
	configs.Assert(ok, "Abort Made Failed")

	CheckPreRead(stmts[0].Manager, txn1, []int{0, 1, 2, 3, 4}, stmts[0].address)
	CheckPreRead(stmts[1].Manager, txn2, []int{0, 1, 2, 3, 4}, stmts[1].address)
	StopServers(stmts)
}
