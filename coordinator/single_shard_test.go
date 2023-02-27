package coordinator

import (
	"FLAC/configs"
	"testing"
)

func TestSingle(t *testing.T) {
	makeLocal()
	defer recLocal()
	ca, cohorts := CoordinatorTestKit()
	w1 := NewTX(1, defaultTimeOUt, address[:1], ca.Manager)
	w2 := NewTX(2, defaultTimeOUt, address[1:2], ca.Manager)
	for i := 0; i < 5; i++ {
		w1.AddUpdate(address[0], uint64(i), i+1)
		w2.AddUpdate(address[1], uint64(i), i+2)
	}
	w1.from.CheckAndChange(1, 0, PreRead)
	w2.from.CheckAndChange(2, 0, PreRead)
	res := ca.Manager.SingleSubmit(nil, w1, nil)
	configs.Assert(res, "The one-shoot Failed")
	res = ca.Manager.SingleSubmit(nil, w2, nil)
	configs.Assert(res, "The one-shoot Failed")
	CheckVal(t, cohorts[0].Manager, []int{1, 2, 3, 4, 5})
	CheckVal(t, cohorts[1].Manager, []int{2, 3, 4, 5, 6})
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}

func TestPreRead(t *testing.T) {
	makeLocal()
	ca, cohorts := CoordinatorTestKit()
	r := NewTX(1, defaultTimeOUt, address, ca.Manager)
	CheckVal(t, cohorts[0].Manager, []int{0, 1, 2, 3, 4})
	CheckVal(t, cohorts[1].Manager, []int{0, 1, 2, 3, 4})
	CheckVal(t, cohorts[2].Manager, []int{0, 1, 2, 3, 4})

	for i := uint64(0); i < 5; i++ {
		r.AddRead(address[0], i)
		r.AddRead(address[1], i)
		r.AddRead(address[2], i)
	}
	txn := NewTX(1, defaultTimeOUt, address, ca.Manager)
	txn.from.CheckAndChange(1, 0, PreRead)
	res, ok := ca.Manager.PreRead(r)
	configs.Assert(ok, "The PreRead Failed")
	for i := 0; i < 5; i++ {
		configs.Assert(res[configs.Hash(address[0], uint64(i))] == i, "Check PreRead value failed")
		configs.Assert(res[configs.Hash(address[1], uint64(i))] == i, "Check PreRead value failed")
		configs.Assert(res[configs.Hash(address[2], uint64(i))] == i, "Check PreRead value failed")
	}
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}

func TestRetry(t *testing.T) {
	makeLocal()
	ca, cohorts := CoordinatorTestKit()
	r := NewTX(1, defaultTimeOUt, address, ca.Manager)
	CheckVal(t, cohorts[0].Manager, []int{0, 1, 2, 3, 4})
	CheckVal(t, cohorts[1].Manager, []int{0, 1, 2, 3, 4})
	CheckVal(t, cohorts[2].Manager, []int{0, 1, 2, 3, 4})

	for i := 0; i < 5; i++ {
		r.AddUpdate(address[0], uint64(i), i)
		r.AddUpdate(address[1], uint64(i), i)
		r.AddUpdate(address[2], uint64(i), i)
	}
	txn := NewTX(1, defaultTimeOUt, address, ca.Manager)
	ca.Manager.TwoPCSubmit(nil, txn, nil)
	ca.Manager.TwoPCSubmit(nil, txn, nil)
	ca.Close()
	for i := 0; i < configs.NumberOfShards; i++ {
		cohorts[i].Close()
	}
}
