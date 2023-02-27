package coordinator

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"time"
)

func (c *FLACManager) sendMsg(server string, mark string, txn network.CoordinatorGossip) {
	configs.DPrintf("TXN" + strconv.FormatUint(txn.TxnID, 10) + ": " + "CA send message for " + server + " with Mark " + mark)
	msg := network.ParticipantGossip{Mark: mark, Txn: txn, SentTime: time.Now()}
	msgBytes, err := json.Marshal(msg)
	configs.CheckError(err)
	c.stmt.conn.sendMsg(server, msgBytes)
}

func (c *FLACManager) sendDecide(server string, pack *network.CoordinatorGossip, isCommit bool) {
	if isCommit {
		c.sendMsg(server, configs.Commit, *pack)
	} else {
		c.sendMsg(server, configs.Abort, *pack)
	}
}

func (c *FLACManager) sendPreWrite(server string, pack *network.CoordinatorGossip) {
	c.sendMsg(server, configs.PreWrite, *pack)
}

func (c *FLACManager) sendAgree(server string, pack *network.CoordinatorGossip, isCommit bool) {
	if isCommit {
		c.sendMsg(server, configs.PreCommit, *pack)
	} else {
		panic("Abort decision passed to the pre-commit")
	}
}

func (txn *TX) sendPreRead(server string, pack *network.CoordinatorGossip) {
	txn.from.sendMsg(server, configs.PreRead, *pack)
}

func (c *FLACManager) sendPropose(server string, pack *network.CoordinatorGossip) {
	c.sendMsg(server, configs.FLACProposed, *pack)
}

func (c *FLACManager) handleFLAC(TID uint64, val *detector.KvRes) {
	c.LockPool[TID].Lock()
	defer c.LockPool[TID].Unlock()
	if c.TxnState[TID] != Propose {
		c.Lsm.AsyNF(val, detector.TimeStamp4NFRec)
		c.toAbnormal(TID)
		return
	}
	c.MsgPool[TID] = append(c.MsgPool[TID], val)
	if val.Persisted { // can enter the slow path.
		c.Decided[TID] = true
	}
}

func (c *FLACManager) handleACK(TID uint64, ack *network.Response4Coordinator) {
	c.LockPool[TID].Lock()
	defer c.LockPool[TID].Unlock()
	state := c.TxnState[TID]
	if ack.Mark == configs.PreWriteACK {
		if state != PreWrite && state != Aborted {
			// it can be aborted by many one
			c.toAbnormal(TID)
			return
		}
		c.MsgPool[TID] = append(c.MsgPool[TID], ack)
		if !ack.ACK {
			c.TxnState[TID] = Aborted
		}
	} else if ack.Mark == configs.Finished {
		if state != Committed && state != Aborted {
			// the error state
			c.toAbnormal(TID)
			return
		}
		if (!ack.ACK && state != Aborted) || (ack.ACK && state != Committed) {
			c.toAbnormal(TID)
			os.Exit(0)
			return
		}
		c.MsgPool[TID] = append(c.MsgPool[TID], ack)
	} else if ack.Mark == configs.PreCommitACK {
		if state != AgCommitted {
			c.toAbnormal(TID)
			return
		}
		c.MsgPool[TID] = append(c.MsgPool[TID], ack)
		if !ack.ACK { // failed txn.
			c.TxnState[TID] = Aborted
		}
	}
}

func (c *FLACManager) handlePreRead(TID uint64, val *map[string]int, ok bool) {
	c.LockPool[TID].Lock()
	defer c.LockPool[TID].Unlock()
	if c.TxnState[TID] != PreRead {
		c.toAbnormal(TID)
		return
	}
	if !ok {
		c.TxnState[TID] = Aborted
		return
	}
	c.MsgPool[TID] = append(c.MsgPool[TID], val)
}

func (txn *TX) check4Response(isCheckSucceed bool, expectCommitted bool) bool {
	txn.from.LockPool[txn.TxnID].Lock()
	defer txn.from.LockPool[txn.TxnID].Unlock()
	state := txn.from.TxnState[txn.TxnID]
	if isCheckSucceed {
		if expectCommitted {
			return len(txn.from.MsgPool[txn.TxnID]) == len(txn.Participants) &&
				state != Aborted && state != Abnormal
		} else {
			return state == Aborted || len(txn.from.MsgPool[txn.TxnID]) == len(txn.Participants)
		}
	} else {
		return state == Abnormal || (expectCommitted && state == Aborted)
	}
}

func (txn *TX) extractResponses4Shard(shardID string) []*network.Response4Coordinator {
	res := make([]*network.Response4Coordinator, 0)
	for _, v := range txn.from.MsgPool[txn.TxnID] {
		cur := v.(*network.Response4Coordinator)
		if cur.ShardID == shardID {
			res = append(res, cur)
		}
	}
	return res
}

func (txn *TX) checkSuperSet(isCheckSucceed bool, expectCommitted bool) bool {
	txn.from.LockPool[txn.TxnID].Lock()
	defer txn.from.LockPool[txn.TxnID].Unlock()
	state := txn.from.TxnState[txn.TxnID]
	if isCheckSucceed {
		for _, shard := range txn.Participants {
			res := txn.extractResponses4Shard(shard)
			shardAgree := 0
			for _, v := range res {
				if v.ACK == false { // no vote received.
					return !expectCommitted
				} else {
					shardAgree++
				}
			}
			if shardAgree < configs.NumberOfReplicas/2+1 {
				// not enough replicas for agreement.
				return false
			}
		}
		return expectCommitted
	} else {
		return state == Abnormal || state == Aborted
	}
}

func (txn *TX) checkSuperMajority(isCheckSucceed bool) bool {
	txn.from.LockPool[txn.TxnID].Lock()
	defer txn.from.LockPool[txn.TxnID].Unlock()
	state := txn.from.TxnState[txn.TxnID]
	if isCheckSucceed {
		agree := 0
		for _, shard := range txn.Participants {
			res := txn.extractResponses4Shard(shard)
			if len(res) >= configs.NumberOfReplicas/2+1 {
				agree++
			}
		}
		return agree >= (len(txn.Participants)+1)/2
	} else {
		return state == Abnormal || state == Aborted
	}
}

func (txn *TX) check4PAC(isCheckSucceed bool) bool {
	txn.from.LockPool[txn.TxnID].Lock()
	defer txn.from.LockPool[txn.TxnID].Unlock()
	state := txn.from.TxnState[txn.TxnID]
	if isCheckSucceed {
		// majority
		return len(txn.from.MsgPool[txn.TxnID]) >= (len(txn.Participants)+1)/2
	} else {
		return state == Abnormal || state == Aborted
	}
}

func (txn *TX) check4Easy() bool {
	txn.from.LockPool[txn.TxnID].Lock()
	defer txn.from.LockPool[txn.TxnID].Unlock()
	return len(txn.from.MsgPool[txn.TxnID]) > 0
}

func (txn *TX) checkFLAC(isCorrect bool) bool {
	txn.from.LockPool[txn.TxnID].Lock()
	defer txn.from.LockPool[txn.TxnID].Unlock()
	state := txn.from.TxnState[txn.TxnID]
	if !isCorrect {
		return state == Abnormal
	} else {
		return len(txn.from.MsgPool[txn.TxnID]) == len(txn.Participants)
	}
}

func (txn *TX) check4Prop(isCorrect bool, lev detector.Level, stillWait bool) bool {
	txn.from.LockPool[txn.TxnID].Lock()
	defer txn.from.LockPool[txn.TxnID].Unlock()
	state := txn.from.TxnState[txn.TxnID]
	if !isCorrect {
		return state != Propose
	} else {
		if !stillWait && ((lev == detector.NoCFNoNF || lev == detector.CFNoNF) && txn.from.Decided[txn.TxnID]) {
			return true
		}
		if len(txn.from.MsgPool[txn.TxnID]) == len(txn.Participants) {
			return true
		}
		return false
	}
}

func (txn *TX) check4Read(isCheckSucceed bool) bool {
	txn.from.LockPool[txn.TxnID].Lock()
	defer txn.from.LockPool[txn.TxnID].Unlock()
	state := txn.from.TxnState[txn.TxnID]
	if isCheckSucceed {
		return len(txn.from.MsgPool[txn.TxnID]) == len(txn.Participants)
	} else {
		return state == Abnormal || state == Aborted
	}
}

func mergeMap(lock *sync.Mutex, maps []interface{}) map[string]int {
	lock.Lock()
	defer lock.Unlock()
	res := make(map[string]int)
	for _, i := range maps {
		p := i.(*map[string]int)
		for x, y := range *p {
			res[x] = y
		}
	}
	return res
}
