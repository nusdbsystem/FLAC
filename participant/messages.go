package participant

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"encoding/json"
	"strconv"
	"time"
)

func (c *Manager) sendKvVote(aim string, vote network.FLACVote) {
	configs.Warn(!configs.EnableReplication, "the key-value vote is not supported for replicated protocols now")
	s := c.stmt.address
	c.PoolLocks[s][vote.TID].Lock()
	defer c.PoolLocks[s][vote.TID].Unlock()
	if c.Pool[s][vote.TID] == nil {
		c.init(vote.TID, vote.From)
	}
	msg := network.ParticipantGossip{
		Mark:     configs.FLACGossipVotes,
		Vt:       vote,
		Txn:      network.CoordinatorGossip{},
		SentTime: time.Now(),
	}
	msgBytes, err := json.Marshal(msg)
	configs.CheckError(err)

	c.stmt.conn.sendMsg(aim, msgBytes)
}

// sendBackCA send back the response message to the coordinator.
func (c *Manager) sendBackCA(TID uint64, shardID, mark string, value interface{}) {
	configs.DPrintf("TXN" + strconv.FormatUint(TID, 10) + ": " + "Send back message from " + c.stmt.address + " with Mark " + mark)
	msg := network.Response4Coordinator{Mark: mark, TID: TID, ShardID: shardID, From: c.stmt.address}
	switch mark {
	case configs.ReadUnsuccessful, configs.ReadSuccess:
		msg.Read = value.(map[string]int)
	case configs.Finished, configs.PreCommitACK, configs.PreWriteACK:
		msg.ACK = value.(bool)
	case configs.FLACResults:
		msg.Res = value.(detector.KvRes)
	}
	msgBytes, err := json.Marshal(msg)
	configs.CheckError(err)
	c.stmt.conn.sendMsg(c.stmt.coordinator, msgBytes)
}

// broadCastVote broadcast votes to other Manager managers.
func (c *Manager) broadCastVote(TID uint64, level detector.Level, vote int, CID string, parts []string) {
	vt := network.FLACVote{
		TID:        TID,
		ACPLevel:   level,
		VoteCommit: vote == 1,
		From:       CID,
	}
	if level == detector.EasyCommit {
		// if the level = EasyCommit, then it serves as the transmission and then decide.
		for _, tp := range parts {
			if tp != CID { // send and then decide.
				c.sendKvVote(tp, vt)
			}
		}
	} else {
		// if the level = NoCFNoNF or CFNoNF, then it designed for vote exchange.
		for _, tp := range parts {
			if tp == CID {
				c.handleVote(&vt)
			} else {
				go c.sendKvVote(tp, vt)
			}
		}
	}
}

// handleVote handles the vote with message buffer for FLAC or handles the decision from other participants.
func (c *Manager) handleVote(vt *network.FLACVote) {
	s := c.stmt.address
	c.PoolLocks[s][vt.TID].Lock()
	defer c.PoolLocks[s][vt.TID].Unlock()
	configs.TxnPrint(vt.TID, strconv.Itoa(int(vt.ACPLevel))+"Vote get From "+vt.From+" to "+c.stmt.address)
	if vt.ACPLevel == detector.EasyCommit {
		// make the same decision with others.
		branch := c.Pool[s][vt.TID]
		if branch == nil {
			return
		}
		if vt.VoteCommit {
			if c.Pool[s][vt.TID] != nil {
				c.Pool[s][vt.TID].Commit(vt.TID)
			}
		} else {
			if c.Pool[s][vt.TID] != nil {
				c.Pool[s][vt.TID].Abort(vt.TID)
			}
		}
	} else if (vt.ACPLevel == detector.NoCFNoNF || vt.ACPLevel == detector.CFNoNF) && !c.haveDecided(s, vt.TID) {
		if c.voteReceived[vt.TID] == nil {
			c.voteReceived[vt.TID] = make([]bool, 0)
		}
		c.voteReceived[vt.TID] = append(c.voteReceived[vt.TID], vt.VoteCommit)
	}
}
