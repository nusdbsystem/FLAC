package mockkv

import (
	"FLAC/configs"
	"FLAC/lock"
	"strconv"
)

type shardEntry struct {
	value int
	addr  int
	mu    *lock.Locker
	from  *Shard
}

func newShardEntry(txnID uint64, value int, addr int, from *Shard) *shardEntry {
	from.logs.appendNoLock(txnID, addr, -1, value)
	return &shardEntry{
		value: value,
		addr:  addr,
		mu:    lock.NewLocker(),
		from:  from,
	}
}

func (s *shardEntry) read(isForce bool) (int, bool) {
	if ok := s.mu.TryRLock(); ok {
		res := s.value
		s.mu.RUnlock()
		return res, true
	} else {
		return -1, false
	}
}

func (s *shardEntry) update(txnID uint64, newVal int) bool {
	if ok := s.mu.TryLock(); ok {
		s.from.logs.appendWrite(txnID, s.addr, s.value, newVal)
		s.value = newVal
		s.mu.Unlock()
		return true
	} else {
		return false
	}
}

func (s *shardEntry) lockRead(txnID uint64, isForce bool) (int, bool) {
	need, _ := s.from.needLock(txnID, s.addr, Read)
	if need { // if no lock has been provided, try to add the lock
		if !s.mu.TryRLock() {
			return -1, false
		}
		s.from.lockPool[txnID].Lock()
		defer s.from.lockPool[txnID].Unlock()
		s.from.addLock(txnID, s.addr, Read, s.mu)
		res := s.value
		return res, true
	} else {
		return s.value, true
	}
}

func (s *shardEntry) lockUpdate(txnID uint64, newVal int) bool {
	need, cur := s.from.needLock(txnID, s.addr, Write)
	if need {
		if cur == Read && !s.mu.UpgradeLock() {
			return false
		} else if cur == None && !s.mu.TryLock() {
			return false
		}
		s.from.lockPool[txnID].Lock()
		defer s.from.lockPool[txnID].Unlock()
		s.from.addLock(txnID, s.addr, Write, s.mu)
		s.from.logs.appendWrite(txnID, s.addr, s.value, newVal)
		configs.TxnPrint(txnID, "f("+strconv.Itoa(s.addr)+"): "+strconv.Itoa(s.value)+" --> "+strconv.Itoa(newVal))
		s.value = newVal
		return true
	} else {
		return true
	}
}
