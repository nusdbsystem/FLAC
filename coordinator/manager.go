package coordinator

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"FLAC/opt"
	"FLAC/participant"
	"FLAC/utils"
	"strconv"
	"sync"
	"time"
)

const (
	PreRead     = 1
	PreWrite    = 2
	Committed   = 3
	Aborted     = 4
	AgCommitted = 5
	Abnormal    = 6
	Propose     = 9
)

// FLACManager serves as a manager of transactions for the coordinator.
type FLACManager struct {
	stmt *Context

	MsgTimeOutBound  time.Duration
	LockTimeOutBound time.Duration
	VoteTimeOutBound time.Duration
	Lsm              *detector.LevelStateManager
	Participants     []string
	Replicas         map[string][]string

	TxnState []int32
	MsgPool  [][]interface{}
	Decided  []bool
	LockPool []*sync.Mutex
	// the msg pool is cleared every time change the state.
	// So that the TX can judge if the message is received by locking at the MsgPool
}

func NewFLACManager(stmt *Context) *FLACManager {
	res := &FLACManager{
		stmt:             stmt,
		Replicas:         stmt.replicas,
		MsgTimeOutBound:  configs.BaseGossipMsgTimeWindow,
		Lsm:              detector.NewLSMManger(stmt.participants),
		Participants:     stmt.participants,
		TxnState:         make([]int32, participant.MaxTxnID),
		MsgPool:          make([][]interface{}, participant.MaxTxnID),
		Decided:          make([]bool, participant.MaxTxnID),
		LockPool:         make([]*sync.Mutex, participant.MaxTxnID),
		VoteTimeOutBound: configs.BaseGossipMsgTimeWindow + configs.OptEps,
	}
	for i := 0; i < participant.MaxTxnID; i++ {
		res.LockPool[i] = &sync.Mutex{}
	}
	return res
}

func (c *FLACManager) start(txnID int) {
	c.TxnState[txnID] = 0
	c.MsgPool[txnID] = make([]interface{}, 0)
}

func (c *FLACManager) release(TID uint64) {
	c.TxnState[TID] = 0
	c.MsgPool[TID] = nil
}

func (c *FLACManager) toAbnormal(TID uint64) {
	c.TxnState[TID] = Abnormal
	c.release(TID)
}

// SkipRead is used when no data needs to be read.
func (c *FLACManager) SkipRead(TID uint64) {
	c.LockPool[TID].Lock()
	defer c.LockPool[TID].Unlock()
	c.TxnState[TID] = PreRead
}

// CheckAndChange compared and change for states of transactions.
func (c *FLACManager) CheckAndChange(TID uint64, begin int32, end int32) bool {
	c.LockPool[TID].Lock()
	defer c.LockPool[TID].Unlock()
	if c.TxnState[TID] == end {
		return true
	}
	if c.TxnState[TID] != begin {
		c.toAbnormal(TID)
		return false
	}
	c.TxnState[TID] = end
	c.MsgPool[TID] = c.MsgPool[TID][:0]
	return true
}

func (c *FLACManager) TrySubmit(txn *TX, protocol string, info *utils.Info) bool {
	defer configs.TimeLoad(time.Now(), "Submit transaction", txn.TxnID, &info.Latency)
	N := len(txn.Participants)
	info.RetryCount = 0
	if N == 1 {
		configs.TPrintf("TXN" + strconv.FormatUint(txn.TxnID, 10) + ": Try with single!!!!")
		// do not use atomic commit protocol for single sharded transactions.
		info.IsCommit = c.SingleSubmit(nil, txn, info)
		return info.IsCommit
	}
	switch protocol {
	case configs.FLAC:
		configs.TPrintf("TXN" + strconv.FormatUint(txn.TxnID, 10) + ": Try with FLAC!!!!")
		info.IsCommit = c.FLACSubmit(nil, txn, info)
	case configs.TwoPC:
		info.IsCommit = c.TwoPCSubmit(nil, txn, info)
	case configs.ThreePC:
		info.IsCommit = c.ThreePCSubmit(nil, txn, info)
	case configs.PAC:
		info.IsCommit = c.PACSubmit(nil, txn, info)
	case configs.EasyCommit:
		info.IsCommit = c.EasySubmit(nil, txn, info)
	case configs.GPAC:
		info.IsCommit = c.GPACSubmit(nil, txn, info)
	default:
		configs.Assert(false, "Incorrect protocol "+protocol)
		return false
	}
	if configs.SimulateClientSideDelay && (protocol != configs.FLAC || info.Failure || (info.Level > 1 && info.IsCommit)) {
		// to simulate 10ms delay between coordinator and client.
		// when protocol = "flac" and we execute with fast path,
		// the results are returned directly from the participants to the coordinator, thus we do not need to wait for it.
		time.Sleep(10 * time.Millisecond)
	}
	return info.IsCommit
}

// SubmitTxn submit a transaction will protocol.
func (c *FLACManager) SubmitTxn(txn *TX, protocol string, info *utils.Info) bool {
	c.SkipRead(txn.TxnID)
	N := len(txn.Participants)
	if info == nil {
		info = utils.NewInfo(N)
	} else {
		info.NumPart = N
	}
	defer configs.TimeLoad(time.Now(), "Submit transaction", txn.TxnID, &info.Latency)
	txn.Optimize()
	configs.TPrintf("TXN" + strconv.FormatUint(txn.TxnID, 10) + ": Begin!!!!")
	res := c.TrySubmit(txn, protocol, info)
	retryTime := info.Latency
	cnt := 1
	retryTime = 40 * time.Millisecond // in our benchmark, we found that 40 would be good for frac and 2pc
	oldTID := txn.TxnID
	for !res && configs.Retry4Abort && info.RetryCount != -1 {
		time.Sleep(retryTime)
		retryTime = time.Duration(float64(retryTime) * 1.2)
		txn.TxnID = utils.GetTxnID()
		c.SkipRead(txn.TxnID)
		configs.TPrintf("TXN" + strconv.FormatUint(txn.TxnID, 10) + ": retrying transaction for TXN:" + strconv.FormatUint(oldTID, 10) + " next " + retryTime.String())
		oldTID = txn.TxnID
		res = c.TrySubmit(txn, protocol, info)
		cnt++
	}
	return res
}

// PreRead pre-read API, currently disabled for test.
func (c *FLACManager) PreRead(txn *TX) (map[string]int, bool) {
	if configs.LockModule == 0 {
		return make(map[string]int), true
	}
	configs.Assert(txn.from.CheckAndChange(txn.TxnID, 0, PreRead), "error status code")
	txn.Optimize()
	branches := make(map[string]*network.CoordinatorGossip)
	for _, v := range txn.Participants {
		branches[v] = network.NewTXPack(txn.TxnID, v, detector.CFNF, txn.Participants)
	}
	for _, v := range txn.OptList {
		switch v.Type {
		case opt.ReadOpt:
			branches[v.Shard].AppendRead(v.Shard, v.Key)
		default:
			configs.Assert(false, "no write should be sent to the PreRead")
		}
	}
	//// build over, broadcast vote ////
	for i, op := range branches {
		go txn.sendPreRead(i, op)
	}
	//// to simulate less blocking, we use 4 * operation timeout.
	waitTime := 2 * (2*txn.TimeOut + configs.LockTimeout + configs.ClientContentionDelay + configs.OptEps)
	for st := time.Now(); time.Since(st) < waitTime; {
		if txn.check4Response(false, true) {
			return nil, false
		}
		if txn.check4Response(true, true) {
			return mergeMap(txn.from.LockPool[txn.TxnID], txn.from.MsgPool[txn.TxnID]), true
		}
		time.Sleep(waitTime / configs.ResponseCheckNumber)
	}
	return nil, false
}

// PDServer is used to manage critical meta-data for the coordinator.
type PDServer struct {
	mu           *sync.Mutex
	state        *detector.LevelStateManager // the state machine Manager
	shardMapper  map[int]int                 // the map from an overall address to the shard id.
	offsetMapper map[int]int                 // the map from an overall address to the offset in the shard.
}

// Register a location in PD.
func (c *PDServer) Register(addr int, shard int, offset int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if val, ok := c.shardMapper[addr]; ok && (val != shard || c.offsetMapper[addr] != offset) {
		return false
	}
	c.shardMapper[addr] = shard
	c.offsetMapper[addr] = offset
	return true
}
