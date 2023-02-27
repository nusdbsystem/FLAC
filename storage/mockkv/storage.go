package mockkv

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/lock"
	"sync"
	"time"
)

const DefaultTimeOut = 500 * time.Microsecond
const MaxTxnID = configs.MaxTID

// Shard maintains a local kv-store and all information needed.
type Shard struct {
	shardID string
	values  []*shardEntry
	mu      *sync.Mutex
	length  int

	logs     *shardLogManager
	rLocks   [][]*lock.Locker
	wLocks   [][]*lock.Locker
	lockMaps []map[int]LogOpt
	lockPool []*sync.Mutex

	TimeOut time.Duration //TimeOut the timeout for locks.
}

func (kv *Shard) GetID() string {
	return kv.shardID
}

func newShardKV(shardID string, len int) *Shard {
	res := &Shard{
		shardID:  shardID,
		values:   make([]*shardEntry, len),
		mu:       &sync.Mutex{},
		length:   len,
		TimeOut:  DefaultTimeOut,
		rLocks:   make([][]*lock.Locker, MaxTxnID),
		wLocks:   make([][]*lock.Locker, MaxTxnID),
		lockMaps: make([]map[int]LogOpt, MaxTxnID),
		lockPool: make([]*sync.Mutex, MaxTxnID),
	}
	res.logs = newShardLogManager(res)
	for i := 0; i < int(MaxTxnID); i++ {
		res.lockPool[i] = &sync.Mutex{}
	}
	for i := 0; i < len; i++ {
		res.values[i] = newShardEntry(NoTXNMark, 0, i, res)
	}
	return res
}

// GetDDL get the deadline for getting lock.
func (kv *Shard) GetDDL() time.Duration {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.TimeOut
}

// SetDDL set the deadline for getting lock.
func (kv *Shard) SetDDL(t time.Duration) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.TimeOut = t
}

// Update time-out update for a[key] = value, return succeed or not. Used for time-limited wait of locks.
func (kv *Shard) Update(key uint64, value int) bool {
	return kv.updateKV(NoTXNMark, key, value, false)
}

// Read a[key], return the value and if success. Used for time-limited wait of locks.
func (kv *Shard) Read(key int) (int, bool) {
	if !configs.Assert(key < kv.length && key >= 0, "RPC Out of Range, plz correct constants/glob_var.go") || kv.values[key] == nil {
		return -1, false
	}
	return kv.values[key].read(true)
}

// For transactions.

// Begin start a transaction with txnID.
func (kv *Shard) Begin(txnID uint64) bool {
	configs.Assert(txnID < MaxTxnID && txnID >= 0, "Invalid transaction ID")
	if !configs.Warn(kv.rLocks[txnID] == nil, "The transaction has been started") {
		return true
	}
	kv.lockPool[txnID].Lock()
	defer kv.lockPool[txnID].Unlock()
	kv.rLocks[txnID] = make([]*lock.Locker, 0)
	kv.wLocks[txnID] = make([]*lock.Locker, 0)
	kv.lockMaps[txnID] = make(map[int]LogOpt)
	kv.logs.logs[txnID] = make([]*kvLog, 0)
	return true
}

// ReadTxn reads an operation for transaction tx and holds the lock.
func (kv *Shard) ReadTxn(txnID uint64, key uint64) (int, bool) {
	if !configs.Assert(int(key) < kv.length && key >= 0, "Key Out of Range") {
		return -1, false
	}
	return kv.values[key].lockRead(txnID, true)
}

// UpdateTxn update an operation for transaction and holds the lock.
func (kv *Shard) UpdateTxn(txnID uint64, key uint64, value int) bool {
	return kv.updateKV(txnID, key, value, true)
}

// Commit the transaction with txnID.
func (kv *Shard) Commit(txnID uint64) bool {
	kv.lockPool[txnID].Lock()
	defer kv.lockPool[txnID].Unlock()
	recLogs := kv.logs.getRecoveryLog(txnID)
	if recLogs != nil {
		kv.mu.Lock()                  // the logs[-1] is a common log, hence lock needed.
		for _, log := range recLogs { // stable the logs.
			log.values[0] = -1
			kv.logs.logs[MaxTxnID-1] = append(kv.logs.logs[MaxTxnID-1], log)
		}
		kv.mu.Unlock()
	}
	kv.release(txnID)
	kv.logs.clear(txnID)
	return true
}

// RollBack the transaction branch txnID. rollback will block all the things.
func (kv *Shard) RollBack(txnID uint64) bool {
	kv.lockPool[txnID].Lock()
	defer kv.lockPool[txnID].Unlock()
	recLogs := kv.logs.getRecoveryLog(txnID)
	if recLogs != nil {
		for i := range recLogs {
			if !configs.Assert(recLogs[len(recLogs)-i-1].undo(kv), "The RollBack failed Because of a failed Force Write.") {
				return false
			}
		}
	}
	kv.logs.clear(txnID)
	kv.release(txnID)
	return true
}

func (kv *Shard) needLock(txnID uint64, key int, opt LogOpt) (bool, LogOpt) {
	configs.Assert(txnID < MaxTxnID && txnID >= 0, "Invalid transaction ID")
	kv.lockPool[txnID].Lock()
	defer kv.lockPool[txnID].Unlock()
	if kv.lockMaps[txnID] == nil {
		return false, LogOpt(-1)
	}
	return kv.lockMaps[txnID][key] < opt, kv.lockMaps[txnID][key]
}

func (kv *Shard) updateKV(txnID uint64, key uint64, value int, holdLock bool) bool {
	if !configs.Assert(key < uint64(kv.length) && key >= 0, "RPC Out of Range, plz correct constants/glob_var.go") {
		return false
	}
	if holdLock {
		return kv.values[key].lockUpdate(txnID, value)
	} else {
		return kv.values[key].update(txnID, value)
	}
}

func (kv *Shard) addLock(txnID uint64, key int, opt LogOpt, locker *lock.Locker) {
	if txnID == NoTXNMark {
		// no record should be stored for the stable logs.
		return
	}
	if kv.lockMaps[txnID] == nil {
		// The transaction is already aborted.
		return
	}
	cur := kv.lockMaps[txnID][key]
	if cur == opt { // if two operations are performed on the same record.
		return
	}
	kv.lockMaps[txnID][key] = LogOpt(detector.Max(int(cur), int(opt)))
	if opt == Read {
		kv.rLocks[txnID] = append(kv.rLocks[txnID], locker)
	} else if opt == Write {
		kv.wLocks[txnID] = append(kv.wLocks[txnID], locker)
	}
}

// release the locks for transaction txnID. Not thread-safe
func (kv *Shard) release(txnID uint64) {
	cnt := 0
	ch := make(chan bool)
	for _, cc := range kv.rLocks[txnID] {
		cnt++
		go func(c *lock.Locker) {
			(*c).RUnlock()
			ch <- true
		}(cc)
	}
	for _, cc := range kv.wLocks[txnID] {
		cnt++
		go func(c *lock.Locker) {
			(*c).Unlock()
			ch <- true
		}(cc)
	}
	kv.rLocks[txnID] = nil
	kv.wLocks[txnID] = nil
	kv.lockMaps[txnID] = nil
	for cnt > 0 {
		configs.Assert(<-ch, "The TryLock Release Got Error")
		cnt--
	}
	configs.TxnPrint(txnID, "The lock is cleared")
}

// read4Undo used for undo.
func (kv *Shard) read4Undo(key int) int {
	if !configs.Assert(key < kv.length && key >= 0, "Key Out of Range") {
		return -1
	}
	return kv.values[key].value
}

// update4Undo used for undo
func (kv *Shard) update4Undo(key int, value int) bool {
	if !configs.Assert(key < kv.length && key >= 0, "Key Out of Range") {
		return false
	}
	kv.values[key].value = value
	return true
}
