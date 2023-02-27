package coordinator

import (
	"FLAC/configs"
	"testing"
)

func TestPACPreWrite(t *testing.T) {
	makeLocal()
	defer recLocal()
	ca, cohorts := CoordinatorTestKit()
	w := NewTX(1, defaultTimeOUt, address, ca.Manager)
	for i := 0; i < 5; i++ {
		w.AddUpdate(address[0], uint64(i), i+1)
		w.AddUpdate(address[1], uint64(i), i+2)
		w.AddUpdate(address[2], uint64(i), i+3)
	}
	txn := NewTX(1, defaultTimeOUt, address, ca.Manager)
	txn.from.CheckAndChange(1, 0, PreRead)
	res := ca.Manager.PACSubmit(nil, w, nil)
	configs.Assert(res, "The C-PAC Failed")
	CheckVal(t, cohorts[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, cohorts[1].Manager, []int{2, 3, 4, 5, 6})
	CheckVal(t, cohorts[2].Manager, []int{3, 4, 5, 6, 7})
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}

func TestPACConcat(t *testing.T) {
	makeLocal()
	defer recLocal()
	ca, cohorts := CoordinatorTestKit()
	w1 := NewTX(1, defaultTimeOUt, address[:1], ca.Manager)
	w2 := NewTX(2, defaultTimeOUt, address[1:], ca.Manager)
	for i := 0; i < 5; i++ {
		w1.AddUpdate(address[0], uint64(i), i+1)
		w2.AddUpdate(address[1], uint64(i), i+2)
		w2.AddUpdate(address[2], uint64(i), i+3)
	}
	w1.from.CheckAndChange(1, 0, PreRead)
	w2.from.CheckAndChange(2, 0, PreRead)
	res := ca.Manager.PACSubmit(nil, w1, nil)
	res = res && ca.Manager.PACSubmit(nil, w2, nil)
	configs.Assert(res, "The C-PAC Failed")
	CheckVal(t, cohorts[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, cohorts[1].Manager, []int{2, 3, 4, 5, 6})
	CheckVal(t, cohorts[2].Manager, []int{3, 4, 5, 6, 7})
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}

func TestPACConcurrent1(t *testing.T) {
	makeLocal()
	defer recLocal()
	ca, cohorts := CoordinatorTestKit()
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
		res := ca.Manager.PACSubmit(nil, w1, nil)
		configs.Assert(res, "The C-PAC Failed")
		ch <- res
	}()
	go func() {
		res := ca.Manager.PACSubmit(nil, w2, nil)
		configs.Assert(res, "The C-PAC Failed")
		ch <- res
	}()
	<-ch
	<-ch
	CheckVal(t, cohorts[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, cohorts[1].Manager, []int{2, 3, 4, 5, 6})
	CheckVal(t, cohorts[2].Manager, []int{3, 4, 5, 6, 7})
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}

func TestPACConcurrent2(t *testing.T) {
	makeLocal()
	defer recLocal()
	ca, cohorts := CoordinatorTestKit()
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
		res := ca.Manager.PACSubmit(nil, w1, nil)
		configs.Assert(res, "The C-PAC Failed")
		ch <- res
	}()
	go func() {
		res := ca.Manager.PACSubmit(nil, w2, nil)
		configs.Assert(res, "The C-PAC Failed")
		ch <- res
	}()
	<-ch
	<-ch
	CheckVal(t, cohorts[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, cohorts[1].Manager, []int{2, 3, 4, 5, 6})
	CheckVal(t, cohorts[2].Manager, []int{3, 4, 5, 6, 7})
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}
