package coordinator

import (
	"FLAC/configs"
	"FLAC/opt"
	"strconv"
	"time"
)

type TX struct {
	TxnID        uint64
	OptList      []opt.TXOpt
	TimeOut      time.Duration
	Participants []string
	from         *FLACManager
}

func NewTX(TID uint64, duration time.Duration, parts []string, from *FLACManager) *TX {
	configs.DPrintf(duration.String())
	return &TX{
		TxnID:        TID,
		OptList:      make([]opt.TXOpt, 0),
		TimeOut:      from.MsgTimeOutBound,
		Participants: parts,
		from:         from,
	}
}

func (txn *TX) AddRead(shard string, key uint64) {
	txn.OptList = append(txn.OptList, opt.TXOpt{
		Shard: shard,
		Key:   key,
		Value: -1,
		Type:  opt.ReadOpt,
	})
}

func (txn *TX) AddUpdate(shard string, key uint64, value interface{}) {
	txn.OptList = append(txn.OptList, opt.TXOpt{
		Shard: shard,
		Key:   key,
		Value: value,
		Type:  opt.UpdateOpt,
	})
}

func hashOpt(sd string, key uint64, val int, cmd uint) string {
	// read write should be supported
	return sd + ";" + strconv.FormatUint(key, 10) + ";" + strconv.Itoa(val) + ";" + strconv.FormatUint(key, 10)
}

// Optimize the transaction to reduce useless operations.
func (txn *TX) Optimize() {
	exist := make(map[string]bool)
	for i := len(txn.OptList) - 1; i >= 0; i-- {
		// TODO: currently use int as value, will change to interface.
		val := hashOpt(txn.OptList[i].Shard, txn.OptList[i].Key, txn.OptList[i].Value.(int), txn.OptList[i].Type)
		if exist[val] {
			txn.OptList = append(txn.OptList[:i], txn.OptList[i+1:]...)
		} else {
			exist[val] = true
		}
	}
}
