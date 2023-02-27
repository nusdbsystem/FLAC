package coordinator

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"FLAC/utils"
	"strconv"
	"time"
)

// Here is a centralized version of PAC. We fix the leader to header node.
// PAC comes to this version if one node is always selected as the leader.

func (txn *TX) FTAgree4PAC(isCommit bool, duration *time.Duration) bool {
	defer configs.TimeAdd(time.Now(), "3PC Agree", txn.TxnID, duration)
	branches := make(map[string]*network.CoordinatorGossip)
	for _, v := range txn.Participants {
		branches[v] = network.NewTXPack(txn.TxnID, v, detector.CFNF, txn.Participants)
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		// Same agreement message reused from 3PC.
		go txn.from.sendAgree(i, op, isCommit)
	}
	configs.DPrintf("begin send agree")
	for st := time.Now(); time.Since(st) < 2*txn.TimeOut+configs.OptEps+configs.ClientContentionDelay; {
		if txn.check4PAC(true) {
			return true
		}
		if txn.check4PAC(false) {
			return false
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

// PACSubmit submit the write only transaction to the KVs with PAC.
func (c *FLACManager) PACSubmit(read *TX, write *TX, info *utils.Info) bool {
	if info == nil {
		info = utils.NewInfo(len(write.Participants))
	}
	configs.Assert(write.from == c, "inconsistent flac manager")
	if !c.CheckAndChange(write.TxnID, PreRead, PreWrite) {
		return false
	}
	// since no leader is changed, no response would come with Decision = true or AcceptVal.
	// the LE + LD phase of PAC degenerate to the same as 3PC.
	ok := write.PreWrite(&info.ST1, info) // non-blocking 2 delays.
	if !ok {
		configs.DPrintf("Txn" + strconv.FormatUint(write.TxnID, 10) + ": failed at pre-write")
		if !c.CheckAndChange(write.TxnID, PreWrite, Aborted) {
			write.DecideBlock(false, &info.ST2) // blocking 2 delays.
			return false
		} else { // abort the transaction directly.
			write.Decide(false, &info.ST2)
			return false
		}
	} else if !c.CheckAndChange(write.TxnID, PreWrite, AgCommitted) {
		write.DecideBlock(false, &info.ST2) // blocking 2 delays.
		return false
	}

	ok = write.FTAgree4PAC(ok, &info.ST2)
	if !ok {
		configs.DPrintf("Txn" + strconv.FormatUint(write.TxnID, 10) + ": failed at agree")
		if !c.CheckAndChange(write.TxnID, AgCommitted, Aborted) {
			write.Decide(false, &info.ST3) // blocking 2 delays.
			return false
		}
	} else if !c.CheckAndChange(write.TxnID, AgCommitted, Committed) {
		write.Decide(false, &info.ST3) // blocking 2 delays.
		return false
	}
	// As mentioned in Paper, the decision is sent asynchronously, but will inform the decision at once.
	go write.DecideBlock(ok, &info.ST3)
	return ok
}
