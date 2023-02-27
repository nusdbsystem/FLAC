package participant

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"github.com/magiconair/properties/assert"
	"testing"
	"time"
)

func CheckVal(t *testing.T, coh *Manager, ans []int) {
	for i := 0; i < len(ans); i++ {
		v, ok := coh.Kv[coh.stmt.address].Read(i)
		configs.JPrint(ok)
		assert.Equal(t, ok && ans[i] == v, true, "CKv : PreRead Failed with different value")
	}
}

func TestConcurrent(t *testing.T) {
	makeLocal()
	stmts := ParticipantsTestKit()
	txn1 := network.NewTXPack(1, stmts[0].address, detector.NoCFNoNF, configs.OuAddress)
	stmts[0].Manager.forTestPreRead(txn1)
	txn2 := network.NewTXPack(2, stmts[0].address, detector.NoCFNoNF, configs.OuAddress)
	stmts[0].Manager.forTestPreRead(txn2)
	CheckVal(t, stmts[0].Manager, []int{0, 1, 2, 3, 4})

	txn1.OptList = txn1.OptList[:0]
	txn2.OptList = txn2.OptList[:0]
	for i := 0; i < 2; i++ {
		txn1.AppendUpdate(stmts[0].address, uint64(i), i+1)
		txn2.AppendUpdate(stmts[0].address, uint64(i)+2, i+1)
	}
	ch := make(chan bool)
	go func() {
		ok := stmts[0].Manager.PreWrite(txn1)
		if !ok {
			println("fail txn1")
		}
		ch <- stmts[0].Manager.Commit(txn1)
	}()
	go func() {
		ok := stmts[0].Manager.PreWrite(txn2)
		if !ok {
			println("fail txn2")
		}
		ch <- stmts[0].Manager.Commit(txn2)
	}()
	<-ch
	<-ch

	CheckVal(t, stmts[0].Manager, []int{1, 2, 1, 2, 4})
	StopServers(stmts)
}

func TestNOCFNONF(t *testing.T) {
	makeLocal()
	stmts := ParticipantsTestKit()
	txn11 := network.NewTXPack(1, stmts[0].address, detector.NoCFNoNF, configs.OuAddress)
	stmts[0].Manager.forTestPreRead(txn11)
	txn12 := network.NewTXPack(1, stmts[1].address, detector.NoCFNoNF, configs.OuAddress)
	stmts[1].Manager.forTestPreRead(txn12)
	CheckVal(t, stmts[0].Manager, []int{0, 1, 2, 3, 4})
	CheckVal(t, stmts[1].Manager, []int{0, 1, 2, 3, 4})

	txn11.OptList = txn11.OptList[:0]
	txn12.OptList = txn12.OptList[:0]
	for i := 0; i < 5; i++ {
		txn11.AppendUpdate(stmts[0].address, uint64(i), i+1)
		txn12.AppendUpdate(stmts[1].address, uint64(i), i+2)
	}
	ch := make(chan bool)
	ans := detector.NewKvResult(2)
	go func() {
		res := stmts[0].Manager.Propose(txn11, time.Now())
		ans.Append(res)
		ch <- true
	}()
	go func() {
		res := stmts[1].Manager.Propose(txn12, time.Now())
		ans.Append(res)
		ch <- true
	}()
	<-ch
	<-ch
	//print(ans.String())
	stmts[0].Manager.Commit(txn11)
	stmts[1].Manager.Commit(txn12)

	CheckVal(t, stmts[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, stmts[1].Manager, []int{2, 3, 4, 5, 6})
	StopServers(stmts)
}

func TestCFNONF(t *testing.T) {
	makeLocal()
	stmts := ParticipantsTestKit()
	txn11 := network.NewTXPack(1, stmts[0].address, detector.CFNoNF, configs.OuAddress[:2])
	stmts[0].Manager.forTestPreRead(txn11)
	txn12 := network.NewTXPack(1, stmts[1].address, detector.CFNoNF, configs.OuAddress[:2])
	stmts[1].Manager.forTestPreRead(txn12)
	CheckVal(t, stmts[0].Manager, []int{0, 1, 2, 3, 4})
	CheckVal(t, stmts[1].Manager, []int{0, 1, 2, 3, 4})

	txn11.OptList = txn11.OptList[:0]
	txn12.OptList = txn12.OptList[:0]
	for i := 0; i < 5; i++ {
		txn11.AppendUpdate(stmts[0].address, uint64(i), i+1)
		txn12.AppendUpdate(stmts[1].address, uint64(i), i+2)
	}
	ch := make(chan bool)
	ans := detector.NewKvResult(2)
	go func() {
		res := stmts[0].Manager.Propose(txn11, time.Now())
		ans.Append(res)
		ch <- true
	}()
	go func() {
		res := stmts[1].Manager.Propose(txn12, time.Now())
		ans.Append(res)
		ch <- true
	}()
	<-ch
	<-ch
	//print(ans.String())
	stmts[0].Manager.Commit(txn11)
	stmts[1].Manager.Commit(txn12)

	CheckVal(t, stmts[0].Manager, []int{1, 2, 3, 4, 5})
	println("here")
	CheckVal(t, stmts[1].Manager, []int{2, 3, 4, 5, 6})
	StopServers(stmts)
}
