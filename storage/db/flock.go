package db

import (
	"FLAC/configs"
	flacErr "FLAC/errors"
	"os"
	"syscall"
	"time"
)

func fLock(file *os.File, exclusive bool, timeout time.Duration) error {
	var curTime = time.Now()
	var err error
	for time.Since(curTime) <= timeout {
		flag := syscall.LOCK_SH
		if exclusive {
			flag = syscall.LOCK_EX
		}
		err = syscall.Flock(int(file.Fd()), flag|syscall.LOCK_NB)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK { // If the file cannot be accessed due to blocking, retry it.
			return err
		}
		// Avoid frequently blocking of resources when timeout is long.
		if configs.FlockDefaultTimeout > timeout/configs.FlockMaximumRetry {
			time.Sleep(configs.FlockDefaultTimeout)
		} else {
			time.Sleep(timeout / configs.FlockMaximumRetry)
		}
	}
	return flacErr.ErrLockTimeout
}
