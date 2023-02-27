package participant

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"FLAC/opt"
	mockkv2 "FLAC/storage/mockkv"
	"fmt"
	"strconv"
	"time"
)

const (
	No  = 0
	Yes = 1
)

// TXNBranch is used to handle one specific transaction.
type TXNBranch struct {
	Kv       *mockkv2.Shard
	Res      *detector.KvRes
	finished bool
	Vote     bool
	from     *Manager
}

func NewParticipantBranch(id uint64, kv *mockkv2.Shard, manager *Manager) *TXNBranch {
	res := &TXNBranch{
		Kv:       kv,
		finished: false,
		Res:      detector.NewKvRes(int(id), kv.GetID()),
		from:     manager,
		Vote:     false,
	}
	return res
}

// Agree agreement or pre-commit phase for 3pc.
func (c *TXNBranch) Agree(TID uint64, isCommit bool) bool {
	c.from.PoolLocks[c.Kv.GetID()][TID].Lock()
	defer c.from.PoolLocks[c.Kv.GetID()][TID].Unlock()
	// either the txn is over, or the result does not match.
	if c.from.haveDecided(c.Kv.GetID(), TID) || c.Vote != isCommit {
		return configs.Warn(false, "3PC report : the agreement failed for "+strconv.FormatUint(TID, 10))
	}
	return true
}

// Propose handles the Propose Phase of FLAC.
func (c *TXNBranch) Propose(tx *network.CoordinatorGossip, sent time.Time) *detector.KvRes {
	defer configs.TimeTrack(time.Now(), "ST1", tx.TxnID)
	configs.Warn(!configs.EnableReplication, "FLAC propose does not support replication!!!")
	configs.TPrintf("TXN" + strconv.FormatUint(tx.TxnID, 10) + ": " + c.from.stmt.address + " Begin propose")
	if !c.GetVote(tx) {
		// if vote to abort, it will abort regardless of the ACPLevel.
		configs.TPrintf("TXN" + strconv.FormatUint(tx.TxnID, 10) + ": " + c.from.stmt.address + " Abort for PreWrite")
		c.Res.SetSelfResult(false, false, true)
		c.from.broadCastVote(tx.TxnID, tx.ProtocolLevel, No, tx.ShardID, tx.ParticipantAddresses)
		configs.Assert(c.from.Abort(tx), "Impossible case, abort failed with all locks")
	} else {
		tim := 2*c.from.stmt.timeoutForMsg + c.from.stmt.timeoutForLocks + configs.KVConcurrencyDelay
		W := tim - time.Since(sent)
		//		println("time window = ", tim.Seconds(), time.Since(sent).Seconds(), sent.Second())
		if tx.ProtocolLevel == detector.CFNoNF {
			configs.DPrintf("TXN" + strconv.FormatUint(tx.TxnID, 10) + ": " + "Yes Voting From " + c.from.stmt.address)
			c.from.broadCastVote(tx.TxnID, detector.CFNoNF, Yes, tx.ShardID, tx.ParticipantAddresses)
			if len(tx.ParticipantAddresses) > 1 &&
				!c.breakableSleep(tx.TxnID, len(tx.ParticipantAddresses), W) {
				c.Res.SetSelfResult(false, false, false) // the transaction decision has been made.
				return c.Res
			}
			if c.from.checkCommit(tx.TxnID, len(tx.ParticipantAddresses)) {
				// if all the votes are received, go to tentative-commit.
				c.Res.SetSelfResult(true, true, false)
			} else {
				configs.TPrintf("TXN" + strconv.FormatUint(tx.TxnID, 10) + ": " + c.from.stmt.address + " Abort for No vote")
				configs.Assert(c.from.Abort(tx), "Impossible case, abort failed with all locks")
				c.Res.SetSelfResult(true, false, true)
			}
		} else if tx.ProtocolLevel == detector.NoCFNoNF {
			configs.DPrintf("TXN" + strconv.FormatUint(tx.TxnID, 10) + ": " + "Yes Voting From " + c.from.stmt.address)
			configs.DPrintf("TXN" + strconv.FormatUint(tx.TxnID, 10) + ": " + " participant side timeout = " + (c.from.stmt.timeoutForMsg + c.from.stmt.timeoutForLocks + configs.KVConcurrencyDelay).String())
			c.from.broadCastVote(tx.TxnID, detector.NoCFNoNF, 1, tx.ShardID, tx.ParticipantAddresses)
			if len(tx.ParticipantAddresses) > 1 &&
				!c.breakableSleep(tx.TxnID, len(tx.ParticipantAddresses), W) {
				c.Res.SetSelfResult(false, false, true) // the transaction decision has been made.
				return c.Res
			}
			if c.from.checkAbort(tx.TxnID) {
				configs.DPrintf("TXN" + strconv.FormatUint(tx.TxnID, 10) + ": " + "Aborted at " + c.from.stmt.address)
				configs.Assert(c.from.Abort(tx), "Impossible case, commit failed with all locks")
				c.Res.SetSelfResult(true, false, true)
			} else if c.from.checkCommit(tx.TxnID, len(tx.ParticipantAddresses)) {
				configs.DPrintf("TXN" + strconv.FormatUint(tx.TxnID, 10) + ": " + "Committed at " + c.from.stmt.address)
				configs.Assert(c.from.Commit(tx), "Impossible case, abort failed with all locks")
				c.Res.SetSelfResult(true, true, true)
			} else {
				configs.DPrintf("TXN" + strconv.FormatUint(tx.TxnID, 10) + ": " + "Pre-Commit at " + c.from.stmt.address + " with " +
					strconv.Itoa(len(c.from.voteReceived[tx.TxnID])) + " votes " + " Expect " + strconv.Itoa(len(tx.ParticipantAddresses)))
				c.Res.SetSelfResult(true, true, false)
			}
		}
	}
	c.from.PoolLocks[c.Kv.GetID()][tx.TxnID].Lock()
	c.finished = true
	c.from.PoolLocks[c.Kv.GetID()][tx.TxnID].Unlock()
	return c.Res
}

// PreWrite get locks and pre-write the changes to temporary logs.
func (c *TXNBranch) PreWrite(tx *network.CoordinatorGossip) bool {
	defer configs.TimeTrack(time.Now(), "PreWrite", tx.TxnID)
	c.Vote = c.GetVote(tx)
	return c.Vote
}

// PreRead handles the read and returns a map containing results.
// tx should only contain read operations.
func (c *TXNBranch) PreRead(tx *network.CoordinatorGossip) (bool, map[string]int) {
	result := make(map[string]int)
	c.Kv.Begin(tx.TxnID)
	for _, v := range tx.OptList {
		switch v.Type {
		case opt.ReadOpt:
			if !c.preRead4Opt(configs.Hash(v.Shard, v.Key), v, tx, &result) {
				return false, result
			}
		default:
			return false, result
		}
	}
	return true, result
}

// Commit the transaction branch.
func (c *TXNBranch) Commit(TxnID uint64) bool {
	defer configs.TimeTrack(time.Now(), "Commit", TxnID)
	return c.Kv.Commit(TxnID)
}

// Abort the transaction branch.
func (c *TXNBranch) Abort(TxnID uint64) bool {
	defer configs.TimeTrack(time.Now(), "Abort", TxnID)
	return c.Kv.RollBack(TxnID)
}

// Check if one transaction exists in the transaction branch pool.
func (c *Manager) exist(shard string, TID int) bool {
	c.PoolLocks[shard][TID].Lock()
	defer c.PoolLocks[shard][TID].Unlock()
	return c.Pool[shard][uint64(TID)] != nil
}

// preRead4Opt the thread-safe pre-read for an operation.
func (c *TXNBranch) preRead4Opt(key string, opt opt.TXOpt,
	tx *network.CoordinatorGossip, values *map[string]int) bool {
	if val, ok := c.Kv.ReadTxn(tx.TxnID, opt.Key); !ok || tx.ShardID != opt.Shard {
		return false
	} else {
		(*values)[key] = val
		return true
	}
}

// preUpdate4Opt the thread-safe pre-update for an operation.
func (c *TXNBranch) preUpdate4Opt(opt opt.TXOpt, tx *network.CoordinatorGossip) bool {
	//	println(c.from.stmt.address)
	v, ok := opt.Value.(int)
	if !ok {
		v = int(opt.Value.(float64))
	}
	if ok = c.Kv.UpdateTxn(tx.TxnID, opt.Key, v); !configs.Warn(ok && tx.ShardID == opt.Shard,
		fmt.Sprintf("The UpdateTxn get aborted %v:%v\n", tx.ShardID, opt.Shard)) {
		return false
	} else {
		return true
	}
}

// GetVote checks if we can get all the locks to ensure ACID for write operations.
// tx should only contain write operations.
func (c *TXNBranch) GetVote(tx *network.CoordinatorGossip) bool {
	// piggyback message
	for _, v := range tx.OptList {
		switch v.Type {
		case opt.UpdateOpt:
			if !c.preUpdate4Opt(v, tx) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// breakableSleep sleep the current thread until timeout or enough information has
// been give to commit/abort the transaction.
func (c *TXNBranch) breakableSleep(TID uint64, N int, duration time.Duration) bool {
	for st := time.Now(); time.Since(st) < duration; {
		if !c.from.exist(c.Kv.GetID(), c.Res.TID) { // add interval to help decrease contention
			return false
		}
		if c.from.checkCommit(TID, N) || c.from.checkAbort(TID) {
			return true
		}
		time.Sleep(duration / time.Duration(N))
	}
	return true
}
