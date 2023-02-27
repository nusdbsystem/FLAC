package coordinator

import (
	"FLAC/configs"
	ds "FLAC/detector"
	"FLAC/network"
	"FLAC/opt"
	"FLAC/utils"
	"time"
)

func (txn *TX) FLACPropose(level ds.Level, duration *time.Duration) *ds.KvResult {
	defer configs.TimeAdd(time.Now(), "FLAC ST1", txn.TxnID, duration)
	branches := make(map[string]*network.CoordinatorGossip)
	for _, v := range txn.Participants {
		branches[v] = network.NewTXPack(txn.TxnID, v, level, txn.Participants)
	}
	for _, v := range txn.OptList {
		switch v.Type {
		case opt.UpdateOpt:
			branches[v.Shard].AppendUpdate(v.Shard, v.Key, v.Value)
		default:
			configs.Assert(false, "no write should be sent to the PreRead")
		}
	}
	for i, op := range branches {
		txn.from.sendPropose(i, op)
	}
	// the no crash failure no network failure level will be blocked here and wait for node recovery.
	// we simulate that block with a long wait time. retry number like block decide.
	if level == ds.NoCFNoNF {
		for st := time.Now(); time.Since(st) < 2*txn.TimeOut+configs.GossipTimeoutWindow+configs.ClientContentionDelay+configs.OptEps+txn.from.LockTimeOutBound+configs.KVConcurrencyDelay; {
			if txn.check4Prop(true, level, true) {
				break
			}
			if txn.check4Prop(false, level, true) {
				// Unexpected error in protocol execution.
				return nil
			}
			time.Sleep(2 * time.Millisecond)
		}
	} else {
		for st := time.Now(); time.Since(st) < 2*txn.TimeOut+configs.GossipTimeoutWindow+configs.ClientContentionDelay+configs.OptEps+txn.from.LockTimeOutBound+configs.KVConcurrencyDelay; {
			if txn.check4Prop(true, level, true) {
				break
			}
			if txn.check4Prop(false, level, true) {
				// Unexpected error in protocol execution.
				return nil
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	N := len(txn.Participants)
	ans := ds.NewKvResult(N)
	txn.from.LockPool[txn.TxnID].Lock()
	for _, v := range txn.from.MsgPool[txn.TxnID] {
		tmp := v.(*ds.KvRes)
		ok := ans.Append(tmp)
		if !configs.Assert(ok, "The append is caught with failure") {
			return nil
		}
	}
	txn.from.MsgPool[txn.TxnID] = txn.from.MsgPool[txn.TxnID][:0]
	txn.from.LockPool[txn.TxnID].Unlock()
	return ans
}

// FLACSubmit submit the write only transaction to the KVs with level 1 ~ 2.
func (c *FLACManager) FLACSubmit(read *TX, write *TX, info *utils.Info) bool {
	if info == nil {
		info = utils.NewInfo(len(write.Participants))
	}
	var ok = false
	defer func() {
		info.IsCommit = ok
		if ok {
			ds.Add_th()
		}
	}()
	if !c.CheckAndChange(write.TxnID, PreRead, Propose) {
		return false
	}
	level, ts := c.Lsm.Start(write.Participants)
	if configs.FLACMinRobustnessLevel < 0 {
		level = ds.Level(-configs.FLACMinRobustnessLevel)
	} else {
		level = ds.MaxLevel(level, ds.Level(configs.FLACMinRobustnessLevel))
	}
	info.Level = int(level)
	configs.TxnPrint(write.TxnID, "ACPLevel = %d", info.Level)

	if level == ds.CFNF {
		configs.CheckError(c.Lsm.Finish(write.Participants, nil, level, ts))
		if !c.CheckAndChange(write.TxnID, Propose, PreRead) {
			return false
		}
		ok = c.EasySubmit(read, write, info)
		return ok
	}
	result := write.FLACPropose(level, &info.ST1)
	if result == nil {
		return false
	}
	if configs.FLACMinRobustnessLevel >= 0 {
		err := c.Lsm.Finish(write.Participants, result, level, ts)
		if !configs.Assert(err == nil, "The RLSM finish caught with error.") {
			return false
		}
	}
	if correctness := result.Correct(level); !correctness {
		info.Failure = true
	}
	if level == ds.NoCFNoNF {
		if result.Decision != ds.UnDecided {
			ok = result.Decision == ds.Commit
			if !result.AllPersisted() { // some are tentative.
				write.Decide(ok, &info.ST2)
			}
			return ok
		} else {
			if result.DecideAllCommit() && result.AppendFinished() {
				write.Decide(true, &info.ST2)
				ok = true
				return ok
			}
			configs.Warn(false, "A transaction is dead now, and needs to be resolved by termination protocol")
			info.RetryCount = -1
			go func() {
				time.Sleep(write.TimeOut * (1 + configs.ACPRetry4NoResponse))
				write.Decide(false, &info.ST2) // currently we simulate the abort after timeout.
			}()
			return false
		}
	} else {
		if !result.AppendFinished() {
			info.RetryCount = -1
		}
		if result.Decision != ds.UnDecided {
			configs.Assert(result.Decision == ds.Abort, "Impossible case for the CFNoNF level")
			if !result.AllPersisted() {
				write.Decide(false, &info.ST2)
			}
			return false
		} else if result.AppendFinished() {
			write.EasyDecide(true, &info.ST2)
			ok = true
			return ok
		} else {
			write.Decide(false, &info.ST2)
			return false
		}
	}
}
