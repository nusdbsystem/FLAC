package participant

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	mockkv2 "FLAC/storage/mockkv"
	"sync"
	"sync/atomic"
	"time"
)

const PREErr = "The preRead is needed"

// Manager manages the rpc calls From others and maintains the shardKv.
type Manager struct {
	stmt         *Context
	Pool         map[string][]*TXNBranch // the first dimension is used for multiple shard inside one node.
	PoolLocks    map[string][]*sync.Mutex
	Kv           map[string]*mockkv2.Shard
	voteReceived [][]bool // no support for vote handler of replicated protocols now.
	broken       int32
	nF           int32
}

const MaxTxnID = int(configs.MaxTID)

// NewParticipantManager create a new participant manager under stmt.
func NewParticipantManager(stmt *Context, storageSize int) *Manager {
	res := &Manager{
		Pool:         make(map[string][]*TXNBranch),
		PoolLocks:    make(map[string][]*sync.Mutex),
		Kv:           make(map[string]*mockkv2.Shard),
		voteReceived: make([][]bool, configs.MaxTID),
		stmt:         stmt,
		broken:       0,
		nF:           0,
	}
	if configs.EnableReplication {
		curPosition := 0
		for i := 0; i < configs.NumberOfShards; i++ {
			if stmt.participants[i] == stmt.address {
				curPosition = i
			}
		}
		for i := 0; i < configs.NumberOfReplicas; i++ {
			// the current nodes contains the data for shard [i] [i-1] ... [i-R+1]
			j := stmt.participants[(curPosition-i+configs.NumberOfShards)%configs.NumberOfShards]
			res.Kv[j] = mockkv2.NewKV(j, storageSize)
			res.Kv[j].SetDDL(stmt.timeoutForLocks)
			res.Pool[j] = make([]*TXNBranch, configs.MaxTID)
			res.PoolLocks[j] = make([]*sync.Mutex, configs.MaxTID)
			for i := 0; i < MaxTxnID; i++ {
				res.PoolLocks[j][i] = &sync.Mutex{}
			}
		}
	} else {
		j := stmt.address
		res.Kv[j] = mockkv2.NewKV(j, storageSize)
		res.Kv[j].SetDDL(stmt.timeoutForLocks)
		res.Pool[j] = make([]*TXNBranch, configs.MaxTID)
		res.PoolLocks[j] = make([]*sync.Mutex, configs.MaxTID)
		for i := 0; i < MaxTxnID; i++ {
			res.PoolLocks[j][i] = &sync.Mutex{}
		}
	}
	return res
}

// PreRead handles the read operations in tx and return results as a map.
func (c *Manager) PreRead(tx *network.CoordinatorGossip) (bool, map[string]int) {
	TID := tx.TxnID
	shard := tx.ShardID
	c.PoolLocks[shard][TID].Lock()
	if !configs.Assert(c.Pool[shard][TID] == nil, PREErr) {
		c.PoolLocks[shard][TID].Unlock()
		return false, nil
	}
	c.PoolLocks[shard][TID].Unlock()
	c.Pool[shard][TID] = NewParticipantBranch(TID, c.Kv[shard], c)
	return c.Pool[shard][TID].PreRead(tx)
}

// PreWrite checks if we can get all the locks for write-only transaction tx to ensure ACID.
func (c *Manager) PreWrite(tx *network.CoordinatorGossip) bool {
	TID := tx.TxnID
	shard := tx.ShardID
	c.PoolLocks[shard][TID].Lock()
	if c.Pool[shard][TID] == nil {
		c.init(TID, tx.ShardID)
	}
	c.PoolLocks[shard][TID].Unlock()
	if c.Pool[shard][TID] == nil {
		return false
	}
	res := c.Pool[shard][TID].PreWrite(tx)
	if tx.ProtocolLevel == detector.NoCFNF && !res {
		// instant abort the transaction locally if the protocol is 2PC
		c.Abort(tx)
	}
	if tx.ProtocolLevel == detector.NoCFNoNF {
		// The PreWrite is not used for FLAC-FF, thus we use
		// this parameter to show that this transaction is a single sharded transaction.
		// the decisions for single sharded transactions are made instantly.
		if !res {
			c.Abort(tx)
		} else {
			c.Commit(tx)
		}
	}
	return res
}

// Agree performs the agreement/pre-commit phase of 3PC.
func (c *Manager) Agree(tx *network.CoordinatorGossip, isCommit bool) bool {
	TID := tx.TxnID
	shard := tx.ShardID
	c.PoolLocks[shard][TID].Lock()
	if c.Pool[shard][TID] == nil {
		c.init(TID, tx.ShardID)
	}
	c.PoolLocks[shard][TID].Unlock()
	return c.Pool[shard][TID].Agree(tx.TxnID, isCommit)
}

// Propose handles the proposal phase of the FLAC algorithm
func (c *Manager) Propose(tx *network.CoordinatorGossip, sent time.Time) *detector.KvRes {
	TID := tx.TxnID
	shard := tx.ShardID
	c.PoolLocks[shard][TID].Lock()
	if c.Pool[shard][TID] == nil {
		c.init(TID, tx.ShardID)
	}
	c.PoolLocks[shard][TID].Unlock()
	return c.Pool[shard][TID].Propose(tx, sent)
}

func (c *Manager) Commit(tx *network.CoordinatorGossip) bool {
	TID := tx.TxnID
	shard := tx.ShardID
	c.PoolLocks[shard][TID].Lock()
	if c.Pool[shard][TID] == nil {
		// the transaction has been committed.
		return true
	}
	c.PoolLocks[shard][TID].Unlock()
	if tx.ProtocolLevel == detector.EasyCommit {
		// transmission and then decide.
		configs.Warn(!configs.EnableReplication, "transmit-and-decide does not support replication!!!")
		c.broadCastVote(TID, detector.EasyCommit, 1, tx.ShardID, tx.ParticipantAddresses)
	}
	branch := c.Pool[shard][TID]
	if branch == nil { // can only happen for replicated protocols.
		configs.Assert(configs.EnableReplication, "impossible case: the transaction branch commit without begin")
		branch = NewParticipantBranch(tx.TxnID, c.Kv[tx.ShardID], c)
		branch.PreWrite(tx)
	}
	res := branch.Commit(tx.TxnID)
	configs.Assert(res, "current logic should not encounter the transaction commit fail")
	c.PoolLocks[shard][TID].Lock()
	c.Pool[shard][TID] = nil
	c.voteReceived[TID] = nil
	c.PoolLocks[shard][TID].Unlock()
	configs.TPrintf("the Commit finishes on manager")
	return res
}

func (c *Manager) Abort(tx *network.CoordinatorGossip) bool {
	TID := tx.TxnID
	shard := tx.ShardID
	c.PoolLocks[shard][TID].Lock()
	if c.Pool[shard][TID] == nil {
		return true
	}
	c.PoolLocks[shard][TID].Unlock()
	if tx.ProtocolLevel == detector.EasyCommit {
		// transmission and then decide.
		configs.Warn(!configs.EnableReplication, "transmit-and-decide does not support replication!!!")
		c.broadCastVote(TID, detector.EasyCommit, 0, tx.ShardID, tx.ParticipantAddresses)
	}
	if TID > configs.MaxTID {
		println(TID)
	}
	var res = true
	branch := c.Pool[shard][TID]
	if branch != nil {
		res = branch.Abort(tx.TxnID)
	}
	c.PoolLocks[shard][TID].Lock()
	c.Pool[shard][TID] = nil
	c.voteReceived[TID] = nil
	c.PoolLocks[shard][TID].Unlock()
	return res
}

// Break the interface to inject crash failure.
func (c *Manager) Break() {
	configs.LPrintf(c.stmt.address + " is crashed !!!!")
	atomic.StoreInt32(&c.broken, 1)
	atomic.StoreInt32(&configs.TestCF, 1)
}

// NetBreak the interface to inject network failure.
func (c *Manager) NetBreak() {
	configs.LPrintf(c.stmt.address + " is network crashed !!!!")
	atomic.StoreInt32(&c.nF, 1)
	atomic.StoreInt32(&configs.TestNF, 1)
}

// Recover the interface to recover from injected crash failure.
func (c *Manager) Recover() {
	configs.LPrintf(c.stmt.address + " is recovered !!!!")
	atomic.StoreInt32(&c.broken, 0)
	atomic.StoreInt32(&configs.TestCF, 0)
}

// NetRecover the interface to recover from injected network failure.
func (c *Manager) NetRecover() {
	configs.LPrintf(c.stmt.address + " is network recovered !!!!")
	atomic.StoreInt32(&c.nF, 0)
	atomic.StoreInt32(&configs.TestNF, 0)
}

func (c *Manager) isBroken() bool {
	return atomic.LoadInt32(&c.broken) == 1
}

func (c *Manager) isNF() bool {
	return atomic.LoadInt32(&c.nF) == 1
}

func (c *Manager) GetStmt() *Context {
	return c.stmt
}

// checkAbort check if the participant c has collected enough information to abort the transaction.
func (c *Manager) checkAbort(TID uint64) bool {
	shard := c.stmt.address
	c.PoolLocks[shard][TID].Lock()
	defer c.PoolLocks[shard][TID].Unlock()
	if c.voteReceived[TID] == nil {
		return false
	}
	for _, op := range c.voteReceived[TID] {
		if !op {
			return true
		}
	}
	return false
}

// checkCommit check if the participant c has collected enough information to commit the transaction.
func (c *Manager) checkCommit(TID uint64, expected int) bool {
	c.PoolLocks[c.stmt.address][TID].Lock()
	defer c.PoolLocks[c.stmt.address][TID].Unlock()
	if c.voteReceived[TID] == nil || len(c.voteReceived[TID]) < expected {
		return false
	}
	for _, op := range c.voteReceived[TID] {
		if !op {
			return false
		}
	}
	return true
}

func (c *Manager) init(TID uint64, shardID string) {
	c.Pool[shardID][TID] = NewParticipantBranch(TID, c.Kv[shardID], c)
	configs.TxnPrint(TID, "transaction begin on node %v with shardID %v", c.stmt.address, shardID)
	c.Kv[shardID].Begin(TID)
}

// haveDecided returns if the transaction has been committed or aborted.
func (c *Manager) haveDecided(shard string, TID uint64) bool {
	return c.Pool[shard][TID] == nil || c.Pool[shard][TID].finished
}

func (c *Manager) forTestPreRead(tx *network.CoordinatorGossip) (bool, map[string]int) {
	TID := tx.TxnID
	c.PoolLocks[tx.ShardID][TID].Lock()
	if c.Pool[tx.ShardID][TID] == nil {
		c.init(TID, tx.ShardID)
	}
	c.PoolLocks[tx.ShardID][TID].Unlock()
	return c.Pool[tx.ShardID][TID].PreRead(tx)
}
