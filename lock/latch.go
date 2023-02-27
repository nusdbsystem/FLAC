package lock

import (
	"sync"
	"time"
)

const (
	MaxTimeOut     = 60 * 60 * 1000 * time.Millisecond
	WriteProtectNs = 5 * 1000
)

func getTimeOut(t time.Duration) time.Duration {
	if t >= 0 {
		return t
	} else {
		return MaxTimeOut
	}
}

type Locker struct {
	read                int
	write               int
	writeProtectEndTime int64
	prevStack           []byte
	mu                  sync.Mutex
}

func (c *Locker) upgradeLock() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.write == 1 || c.read > 1 {
		// avoid write lock starvation caused by multiple read lock requests.
		c.writeProtectEndTime = time.Now().UnixNano() + WriteProtectNs
		return false
	}
	c.write = 1
	c.read = 0
	c.writeProtectEndTime = time.Now().UnixNano()
	return true
}

func (c *Locker) UpgradeLock() bool {
	return c.upgradeLock()
}

func (c *Locker) lock() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.write == 1 || c.read > 0 {
		// avoid write lock starvation caused by multiple read lock requests.
		c.writeProtectEndTime = time.Now().UnixNano() + WriteProtectNs
		return false
	}
	c.write = 1
	c.writeProtectEndTime = time.Now().UnixNano()
	return true
}

func (c *Locker) TryLock() bool {
	return c.lock()
}

func (c *Locker) Lock() {
	for !c.TryLock() {
	}
}

func (c *Locker) Unlock() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.write = 0
}

func (c *Locker) rLock() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.write == 1 || time.Now().UnixNano() < c.writeProtectEndTime {
		return false
	}
	c.read += 1

	return true
}

func (c *Locker) TryRLock() bool {
	return c.rLock()
}

func (c *Locker) RLock() {
	for !c.TryRLock() {
	}
}

func (c *Locker) RUnlock() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.read > 0 {
		c.read--
	}
}

func NewLocker() *Locker {
	return &Locker{}
}
