package coordinator

import (
	"FLAC/configs"
	"FLAC/participant"
	"github.com/magiconair/properties/assert"
	"strconv"
	"testing"
	"time"
)

var address = []string{"127.0.0.1:6001", "127.0.0.1:6002", "127.0.0.1:6003"}

const defaultTimeOUt = 100 * time.Millisecond

var buf = true

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
	buf = configs.LocalTest
}

func recLocal() {
	configs.LocalTest = buf
}

func CoordinatorTestKit() (*Context, []*participant.Context) {
	stmt := &Context{}
	caID := "127.0.0.1:5001"
	var Arg = []string{"*", "*", caID}
	ch := make(chan bool)
	go begin(stmt, Arg, ch)
	<-ch

	return stmt, participant.OUParticipantsTestKit()
}

func CheckVal(t *testing.T, coh *participant.Manager, ans []int) {
	for i := 0; i < len(ans); i++ {
		v, ok := coh.Kv[coh.GetStmt().GetAddr()].Read(i)
		for !ok {
			v, ok = coh.Kv[coh.GetStmt().GetAddr()].Read(i)
			time.Sleep(100 * time.Millisecond)
		}
		assert.Equal(t, ok && ans[i] == v, true, "CKv : PreRead Failed with different value "+strconv.Itoa(v)+" - "+strconv.Itoa(ans[i]))
	}
}

func Test2PCPreWrite(t *testing.T) {
	makeLocal()
	defer recLocal()
	ca, co := CoordinatorTestKit()
	w := NewTX(1, defaultTimeOUt, address, ca.Manager)
	for i := 0; i < 5; i++ {
		w.AddUpdate(address[0], uint64(i), i+1)
		w.AddUpdate(address[1], uint64(i), i+2)
		w.AddUpdate(address[2], uint64(i), i+3)
	}
	txn := NewTX(1, defaultTimeOUt, address, ca.Manager)
	txn.from.CheckAndChange(1, 0, PreRead)
	res := ca.Manager.TwoPCSubmit(nil, w, nil)
	configs.Assert(res, "The 2PC Failed")
	CheckVal(t, co[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, co[1].Manager, []int{2, 3, 4, 5, 6})
	CheckVal(t, co[2].Manager, []int{3, 4, 5, 6, 7})
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		co[i].Close()
	}
}

func Test2PCConcurrent(t *testing.T) {
	makeLocal()
	defer recLocal()
	ca, co := CoordinatorTestKit()
	w1 := NewTX(1, defaultTimeOUt, address[:1], ca.Manager)
	w2 := NewTX(2, defaultTimeOUt, address[1:], ca.Manager)
	for i := 0; i < 5; i++ {
		w1.AddUpdate(address[0], uint64(i), i+1)
		w2.AddUpdate(address[1], uint64(i), i+2)
		w2.AddUpdate(address[2], uint64(i), i+3)
	}
	w1.from.CheckAndChange(1, 0, PreRead)
	w2.from.CheckAndChange(2, 0, PreRead)
	res := ca.Manager.TwoPCSubmit(nil, w1, nil)
	res = res && ca.Manager.TwoPCSubmit(nil, w2, nil)
	configs.Assert(res, "The 2PC Failed")
	CheckVal(t, co[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, co[1].Manager, []int{2, 3, 4, 5, 6})
	CheckVal(t, co[2].Manager, []int{3, 4, 5, 6, 7})
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		co[i].Close()
	}
}

func Test2PCConcurrent1(t *testing.T) {
	makeLocal()
	defer recLocal()
	ca, co := CoordinatorTestKit()
	w1 := NewTX(1, defaultTimeOUt, address[:1], ca.Manager)
	w2 := NewTX(2, defaultTimeOUt, address[1:], ca.Manager)
	for i := 0; i < 5; i++ {
		w1.AddUpdate(address[0], uint64(i), i+1)
		w2.AddUpdate(address[1], uint64(i), i+2)
		w2.AddUpdate(address[2], uint64(i), i+3)
	}
	w1.from.CheckAndChange(1, 0, PreRead)
	w2.from.CheckAndChange(2, 0, PreRead)
	ch := make(chan bool)
	go func() {
		res := ca.Manager.TwoPCSubmit(nil, w1, nil)
		configs.Assert(res, "The 2PC Failed")
		ch <- res
	}()
	go func() {
		res := ca.Manager.TwoPCSubmit(nil, w2, nil)
		configs.Assert(res, "The 2PC Failed")
		ch <- res
	}()
	<-ch
	<-ch
	CheckVal(t, co[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, co[1].Manager, []int{2, 3, 4, 5, 6})
	CheckVal(t, co[2].Manager, []int{3, 4, 5, 6, 7})
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		co[i].Close()
	}
}

func Test2PCConcurrent2(t *testing.T) {
	makeLocal()
	defer recLocal()
	ca, co := CoordinatorTestKit()
	w1 := NewTX(1, defaultTimeOUt, address, ca.Manager)
	w2 := NewTX(2, defaultTimeOUt, address, ca.Manager)
	for i := 0; i < 2; i++ {
		w1.AddUpdate(address[0], uint64(i), i+1)
		w1.AddUpdate(address[1], uint64(i), i+2)
		w1.AddUpdate(address[2], uint64(i), i+3)
		w2.AddUpdate(address[0], uint64(i)+2, i+3)
		w2.AddUpdate(address[1], uint64(i)+2, i+4)
		w2.AddUpdate(address[2], uint64(i)+2, i+5)
	}
	w2.AddUpdate(address[0], 4, 5)
	w2.AddUpdate(address[1], 4, 6)
	w2.AddUpdate(address[2], 4, 7)
	w1.from.CheckAndChange(1, 0, PreRead)
	w2.from.CheckAndChange(2, 0, PreRead)
	ch := make(chan bool)
	go func() {
		res := ca.Manager.TwoPCSubmit(nil, w1, nil)
		configs.Assert(res, "The 2PC Failed")
		ch <- res
	}()
	go func() {
		res := ca.Manager.TwoPCSubmit(nil, w2, nil)
		configs.Assert(res, "The 2PC Failed")
		ch <- res
	}()
	<-ch
	<-ch
	CheckVal(t, co[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, co[1].Manager, []int{2, 3, 4, 5, 6})
	CheckVal(t, co[2].Manager, []int{3, 4, 5, 6, 7})
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		co[i].Close()
	}
}
