package coordinator

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"FLAC/opt"
	"FLAC/utils"
	"time"
)

// EasyDecide The easy decide uses the technique: transmission and then decide without message lost
// this can ensure the safety and liveness of ACP in the decide phase.
func (txn *TX) EasyDecide(isCommit bool, duration *time.Duration) bool {
	defer configs.TimeAdd(time.Now(), "EasyDecide", txn.TxnID, duration)
	branches := make(map[string]*network.CoordinatorGossip)
	for _, v := range txn.Participants {
		branches[v] = network.NewTXPack(txn.TxnID, v, detector.EasyCommit, txn.Participants)
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
	// as mentioned in the original paper <implementation section>,
	// no need to wait for reply, but must have messages sent first.
	for i, op := range branches {
		txn.from.sendDecide(i, op, isCommit)
	}
	return isCommit
}

// EasySubmit submit write-only transaction to the KVs with Easy Commit protocol.
func (c *FLACManager) EasySubmit(read *TX, write *TX, info *utils.Info) bool {
	if info == nil {
		info = utils.NewInfo(len(write.Participants))
	}
	if !c.CheckAndChange(write.TxnID, PreRead, PreWrite) {
		return false
	}
	ok := write.PreWrite(&info.ST1, info) // same first phase with 2PC.
	if !ok {
		configs.TxnPrint(write.TxnID, "failed at pre-write")
		if !configs.Assert(write.from.CheckAndChange(write.TxnID, PreWrite, Aborted), "incorrect decision for pre-write") {
			write.DecideBlock(false, &info.ST3)
			return false
		}
	} else if !c.CheckAndChange(write.TxnID, PreWrite, Committed) {
		write.DecideBlock(false, &info.ST3)
		return false
	}
	return write.EasyDecide(ok, &info.ST2)
}
