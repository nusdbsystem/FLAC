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

// Here is G-PAC. We fix the leader to the coordinator.
// Here is a generalized + replicated version of PAC. We fix the leader to the coordinator node.

// FTAgree4GPAC get agreement from a super-set.
func (txn *TX) FTAgree4GPAC(isCommit bool, duration *time.Duration) bool {
	defer configs.TimeAdd(time.Now(), "3PC Agree", txn.TxnID, duration)
	branches := make(map[string]*network.CoordinatorGossip)
	for _, v := range txn.Participants {
		configs.Assert(len(txn.from.Replicas[v]) == configs.NumberOfReplicas, "not enough replicas")
		for _, r := range txn.from.Replicas[v] {
			branches[v+"-"+r] = network.NewReplicatedTXPack(txn.TxnID, v, r, detector.CFNF, txn.Participants)
		}
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		// Same agreement message reused from 3PC.
		go txn.from.sendAgree(extractAimAddress4Branch(i), op, isCommit)
	}
	configs.DPrintf("begin send agree")
	for st := time.Now(); time.Since(st) < 2*txn.TimeOut+configs.OptEps+configs.ClientContentionDelay; {
		if txn.checkSuperMajority(true) {
			return true
		}
		if txn.checkSuperMajority(false) {
			return false
		}
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

func extractAimAddress4Branch(branchName string) string {
	i := 0
	for branchName[i] != '-' {
		i++
	}
	return branchName[:i]
}

// PreWriteSuperSet pre-write phase of G-PAC to retrieve votes from Participants.
func (txn *TX) PreWriteSuperSet(duration *time.Duration, info *utils.Info) bool {
	defer configs.TimeAdd(time.Now(), "G-PAC PreWrite", txn.TxnID, duration)
	branches := make(map[string]*network.CoordinatorGossip)
	for _, v := range txn.Participants {
		configs.Assert(len(txn.from.Replicas[v]) == configs.NumberOfReplicas, "not enough replicas")
		for _, r := range txn.from.Replicas[v] {
			branches[v+"-"+r] = network.NewReplicatedTXPack(txn.TxnID, v, r, detector.CFNF, txn.Participants)
		}
	}
	for _, v := range txn.OptList {
		switch v.Type {
		case opt.UpdateOpt:
			configs.Assert(len(txn.from.Replicas[v.Shard]) == configs.NumberOfReplicas, "not enough replicas")
			for _, r := range txn.from.Replicas[v.Shard] { // add to all branches.
				branches[v.Shard+"-"+r].AppendUpdate(v.Shard, v.Key, v.Value)
			}
		default:
			configs.Assert(false, "no write should be sent to the PreRead")
		}
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		go txn.from.sendPreWrite(extractAimAddress4Branch(i), op)
	}
	for st := time.Now(); time.Since(st) < 2*txn.TimeOut+configs.OptEps+configs.ClientContentionDelay+txn.from.LockTimeOutBound; {
		if txn.checkSuperSet(true, true) { // can commit
			return true
		}
		if txn.checkSuperSet(false, false) { // abnormal
			return false
		}
		if txn.checkSuperSet(true, false) { // abort ack received
			return false
		}
		time.Sleep(1 * time.Millisecond)
	}
	info.RetryCount = -1
	return false
}

func (txn *TX) DecideReplicated(isCommit bool, duration *time.Duration) {
	defer configs.TimeAdd(time.Now(), "Decide Block", txn.TxnID, duration)
	branches := make(map[string]*network.CoordinatorGossip)
	for _, v := range txn.Participants {
		configs.Assert(len(txn.from.Replicas[v]) == configs.NumberOfReplicas, "not enough replicas")
		for _, r := range txn.from.Replicas[v] {
			branches[v+"-"+r] = network.NewReplicatedTXPack(txn.TxnID, v, r, detector.CFNF, txn.Participants)
		}
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		txn.from.sendDecide(extractAimAddress4Branch(i), op, isCommit)
	}
	// we simulate that block with a long wait time. retry number like block decide.
	optt := 1 + configs.ACPRetry4NoResponse
	for st := time.Now(); time.Since(st) < time.Duration(optt)*(2*txn.TimeOut+configs.OptEps+configs.ClientContentionDelay); {
		if txn.checkSuperSet(true, false) {
			return
		}
		if txn.checkSuperSet(false, false) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// GPACSubmit submit the write only transaction to the KVs with PAC.
func (c *FLACManager) GPACSubmit(read *TX, write *TX, info *utils.Info) bool {
	if info == nil {
		info = utils.NewInfo(len(write.Participants))
	}
	configs.Assert(write.from == c, "inconsistent flac manager")
	if !c.CheckAndChange(write.TxnID, PreRead, PreWrite) {
		return false
	}
	// since no leader is changed, no response would come with Decision = true or AcceptVal.
	// the LE + LD phase of PAC degenerate to the same as 3PC.
	ok := write.PreWriteSuperSet(&info.ST1, info) // non-blocking 2 delays.
	if !ok {
		configs.DPrintf("Txn" + strconv.FormatUint(write.TxnID, 10) + ": failed at pre-write")
		if !c.CheckAndChange(write.TxnID, PreWrite, Aborted) {
			write.DecideReplicated(false, &info.ST2) // blocking 2 delays.
			return false
		} else { // abort the transaction directly.
			write.DecideReplicated(false, &info.ST2)
			return false
		}
	} else if !c.CheckAndChange(write.TxnID, PreWrite, AgCommitted) {
		write.DecideReplicated(false, &info.ST2) // blocking 2 delays.
		return false
	}

	ok = write.FTAgree4GPAC(ok, &info.ST2)
	if !ok {
		configs.DPrintf("Txn" + strconv.FormatUint(write.TxnID, 10) + ": failed at agree")
		if !c.CheckAndChange(write.TxnID, AgCommitted, Aborted) {
			write.DecideReplicated(false, &info.ST3) // blocking 2 delays.
			return false
		}
	} else if !c.CheckAndChange(write.TxnID, AgCommitted, Committed) {
		write.DecideReplicated(false, &info.ST3) // blocking 2 delays.
		return false
	}
	// As mentioned in Paper, the decision is sent asynchronously, but will inform the decision at once.
	go write.DecideReplicated(ok, &info.ST3)
	return ok
}
