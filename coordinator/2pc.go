package coordinator

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"FLAC/opt"
	"FLAC/utils"
	"strconv"
	"time"
)

// DecideBlock sends the decision and requires ACKs to end.
func (txn *TX) DecideBlock(isCommit bool, duration *time.Duration) bool {
	defer configs.TimeAdd(time.Now(), "Decide Block", txn.TxnID, duration)
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
		go txn.from.sendDecide(i, op, isCommit)
	}
	// we simulate that block with a long wait time. retry number like block decide.
	optt := 1 + configs.ACPRetry4NoResponse
	for st := time.Now(); time.Since(st) < time.Duration(optt)*(2*txn.TimeOut+configs.OptEps+configs.ClientContentionDelay); {
		if txn.check4Response(true, false) {
			return true
		}
		if txn.check4Response(false, false) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

// PreWrite pre-write phase of 2PC, 3PC, EasyCommit to retrieve votes from Participants.
func (txn *TX) PreWrite(duration *time.Duration, info *utils.Info) bool {
	defer configs.TimeAdd(time.Now(), "3PC/2PC PreWrite", txn.TxnID, duration)
	branches := make(map[string]*network.CoordinatorGossip)
	for _, v := range txn.Participants {
		branches[v] = network.NewTXPack(txn.TxnID, v, detector.NoCFNF, txn.Participants)
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
		if txn.check4Response(true, true) { // can commit
			return true
		}
		if txn.check4Response(false, false) { // abnormal
			return false
		}
		if txn.check4Response(true, false) { // abort ack received
			return false
		}
		time.Sleep(1 * time.Millisecond)
	}
	info.RetryCount = -1
	return false
}

// TwoPCSubmit submit the write-only transaction to the KVs with 2PC.
func (c *FLACManager) TwoPCSubmit(read *TX, write *TX, info *utils.Info) bool {
	if info == nil {
		info = utils.NewInfo(len(write.Participants))
	}
	if !write.from.CheckAndChange(write.TxnID, PreRead, PreWrite) {
		return false
	}
	ok := write.PreWrite(&info.ST1, info) // Prepare message
	if !ok {
		configs.DPrintf("Txn" + strconv.FormatUint(write.TxnID, 10) + ": failed at pre-write")
		if !write.from.CheckAndChange(write.TxnID, PreWrite, Aborted) {
			write.DecideBlock(false, &info.ST2)
			return false
		}
	} else if !write.from.CheckAndChange(write.TxnID, PreWrite, Committed) {
		write.DecideBlock(false, &info.ST2)
		return false
	}
	if !write.DecideBlock(ok, &info.ST2) && ok {
		info.RetryCount = -1
		info.Failure = true
	}
	// 2PC needs to receive all the ACKs.
	return ok
}
