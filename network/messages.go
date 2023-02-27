package network

import (
	"FLAC/detector"
	"FLAC/opt"
	"time"
)

// CoordinatorGossip pack transaction information for transportation in ACP.
type CoordinatorGossip struct {
	TxnID                uint64
	ShardID              string
	To                   string
	ProtocolLevel        detector.Level
	OptList              []opt.TXOpt
	ParticipantAddresses []string
}

type Response4Coordinator struct {
	TID     uint64
	Mark    string
	ShardID string
	From    string
	Res     detector.KvRes
	Read    map[string]int
	ACK     bool
}

type FLACVote struct {
	TID        uint64
	ACPLevel   detector.Level
	From       string
	VoteCommit bool
}

// ParticipantGossip pack gossip messages between participants.
type ParticipantGossip struct {
	Mark     string
	Txn      CoordinatorGossip
	Vt       FLACVote
	SentTime time.Time
}

// NewReplicatedTXPack create transaction branch for a replica to execute.
func NewReplicatedTXPack(TID uint64, shardID string, replicaID string, level detector.Level, parts []string) *CoordinatorGossip {
	res := &CoordinatorGossip{
		TxnID:                TID,
		ShardID:              shardID,
		To:                   replicaID,
		ProtocolLevel:        level,
		OptList:              make([]opt.TXOpt, 0),
		ParticipantAddresses: parts,
	}
	return res
}

func NewTXPack(TID uint64, shardID string, level detector.Level, parts []string) *CoordinatorGossip {
	res := &CoordinatorGossip{
		TxnID:                TID,
		ShardID:              shardID,
		To:                   shardID,
		ProtocolLevel:        level,
		OptList:              make([]opt.TXOpt, 0),
		ParticipantAddresses: parts,
	}
	return res
}

func (c *CoordinatorGossip) AppendRead(shard string, key uint64) {
	c.OptList = append(c.OptList, opt.TXOpt{
		Shard: shard,
		Key:   key,
		Value: -1,
		Type:  opt.ReadOpt,
	})
}

func (c *CoordinatorGossip) AppendUpdate(shard string, key uint64, value interface{}) {
	c.OptList = append(c.OptList, opt.TXOpt{
		Shard: shard,
		Key:   key,
		Value: value,
		Type:  opt.UpdateOpt,
	})
}

func (c *ParticipantGossip) String() string {
	return c.Mark
}
