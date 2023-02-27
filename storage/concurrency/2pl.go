package cc

import (
	"github.com/viney-shih/go-lock"
	"time"
)

// NO_WAIT, if a transaction tries to access a locked record and the lock mode is not compatible with the requested mode, then
// the DBMS aborts the transaction that is requesting the lock

const timeOutLock42PL = 2 * time.Millisecond

type TwoPLNoWait struct {
	mu lock.RWMutex
}
