package coordinator

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"FLAC/opt"
	"FLAC/utils"
	"time"
)

// OneShoot commit/abort the transaction located in a single shard with 1RRT.
func (txn *TX) OneShoot(info *utils.Info) bool {
	defer configs.TimeLoad(time.Now(), "One shoot", txn.TxnID, &info.Latency)
	branches := make(map[string]*network.CoordinatorGossip)
	configs.Assert(len(txn.Participants) == 1, "A cross shard transaction misses atomic commit protocol")
	for _, v := range txn.Participants {
		// mark for single sharded transaction.
		branches[v] = network.NewTXPack(txn.TxnID, v, detector.NoCFNoNF, txn.Participants)
	}
	for _, v := range txn.OptList {
		switch v.Type {
		case opt.UpdateOpt:
			branches[v.Shard].AppendUpdate(v.Shard, v.Key, v.Value)
		default:
			configs.Assert(false, "no write should be sent to the PreRead")
		}
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		go txn.from.sendPreWrite(i, op)
	}
	for st := time.Now(); time.Since(st) < 2*txn.TimeOut+configs.OptEps+configs.ClientContentionDelay+txn.from.LockTimeOutBound; {
		if txn.check4Response(true, true) {
			return true
		}
		if txn.check4Response(false, false) {
			return false
		}
		if txn.check4Response(true, false) {
			return false
		}
		time.Sleep(1 * time.Millisecond)
	}
	info.RetryCount = -1
	return false
}

// SingleSubmit submit write-only transaction to a single KV with one RRT.
func (c *FLACManager) SingleSubmit(read *TX, write *TX, info *utils.Info) bool {
	if info == nil {
		info = utils.NewInfo(len(write.Participants))
	}
	if !c.CheckAndChange(write.TxnID, PreRead, PreWrite) {
		return false
	}
	return write.OneShoot(info)
}
