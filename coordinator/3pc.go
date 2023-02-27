package coordinator

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"FLAC/utils"
	"time"
)

func (txn *TX) Agree(isCommit bool, duration *time.Duration) bool {
	defer configs.TimeAdd(time.Now(), "3PC Agree", txn.TxnID, duration)
	branches := make(map[string]*network.CoordinatorGossip)
	for _, v := range txn.Participants {
		branches[v] = network.NewTXPack(txn.TxnID, v, detector.CFNF, txn.Participants)
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		go txn.from.sendAgree(i, op, isCommit)
	}
	configs.DPrintf("begin send agree")
	for st := time.Now(); time.Since(st) < 2*txn.TimeOut+configs.OptEps+configs.ClientContentionDelay; {
		if txn.check4Response(true, true) {
			return true
		}
		if txn.check4Response(false, true) {
			return false
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

// Decide process decide phase for all protocols.
func (txn *TX) Decide(isCommit bool, duration *time.Duration) {
	defer configs.TimeAdd(time.Now(), "Decide", txn.TxnID, duration)
	branches := make(map[string]*network.CoordinatorGossip)
	for _, v := range txn.Participants {
		branches[v] = network.NewTXPack(txn.TxnID, v, detector.CFNF, txn.Participants)
	}
	//for _, v := range txn.OptList {
	//	switch v.Type {
	//	case opt.UpdateOpt:
	//		branches[v.Shard].AppendUpdate(v.Shard, v.Key, v.Value)
	//	default:
	//		configs.Assert(false, "no write should be sent to the PreRead")
	//	}
	//}
	//// build over, broadcast vote ////
	for i, op := range branches {
		txn.from.sendDecide(i, op, isCommit)
	}
}

// ThreePCSubmit  submit write-only transaction to the KVs with 3PC.
func (c *FLACManager) ThreePCSubmit(read *TX, write *TX, info *utils.Info) bool {
	if info == nil {
		info = utils.NewInfo(len(write.Participants))
	}
	if !c.CheckAndChange(write.TxnID, PreRead, PreWrite) {
		return false
	}
	ok := write.PreWrite(&info.ST1, info)
	if !ok {
		configs.TxnPrint(write.TxnID, "failed at pre-write")
		if !c.CheckAndChange(write.TxnID, PreWrite, Aborted) {
			write.DecideBlock(false, &info.ST2)
			return false
		} else { // abort the transaction directly.
			write.Decide(false, &info.ST2)
			return false
		}
	} else if !c.CheckAndChange(write.TxnID, PreWrite, AgCommitted) {
		write.DecideBlock(false, &info.ST2) // blocking 2 delays.
		return false
	}

	ok = write.Agree(ok, &info.ST2)
	if !ok {
		configs.TxnPrint(write.TxnID, "failed at agree")
		if !c.CheckAndChange(write.TxnID, AgCommitted, Aborted) {
			write.DecideBlock(false, &info.ST3) // blocking 2 delays.
			return false
		}
	} else if !c.CheckAndChange(write.TxnID, AgCommitted, Committed) {
		write.DecideBlock(false, &info.ST3) // blocking 2 delays.
		return false
	}
	write.Decide(ok, &info.ST3) // It does not matter if the decide is finished since the agree is correct.
	return ok
}
