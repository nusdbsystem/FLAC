package participant

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func txnAccess(stmt *Context, n, rang int, txnID uint64) bool {
	configs.TimeTrack(time.Now(), "txnAccess in participant", txnID)
	tx := network.NewTXPack(txnID, stmt.address, detector.NoCFNoNF, []string{stmt.address})
	for i := 0; i < n; i++ {
		tx.AppendUpdate(stmt.address, uint64(rand.Intn(rang)+1), rand.Intn(10000))
	}
	//fmt.Println(stmt.timeoutForLocks)
	//fmt.Printf("executing transaction id = %v\n", txnID)
	return stmt.Manager.PreWrite(tx)
}

func TestTxN(t *testing.T) {
	makeLocal()
	stmt := &Context{}
	initData(stmt, "127.0.0.1:6001")
	rand.Seed(time.Now().UnixNano())
	for con := 1; con < 65; con *= 2 {
		st := time.Now()
		suc := int32(0)
		ch := make(chan bool, con)
		for c := 0; c < con; c++ {
			go func(done chan bool, pos int) {
				for i := 0; i < configs.SpeedTestBatchPerThread; i++ {
					if txnAccess(stmt, 5, configs.NumberOfRecordsPerShard-1, uint64(i+configs.SpeedTestBatchPerThread*pos)) {
						atomic.AddInt32(&suc, 1)
					}
				}
				done <- true
			}(ch, c)
		}
		for i := 0; i < con; i++ {
			<-ch
		}
		bas := time.Since(st).Seconds()
		fmt.Printf("with %v concurrent threads, %.2f local transactions executed in one second, %.2f success\n",
			con, float64(configs.SpeedTestBatchPerThread)*float64(con)/bas, float64(suc)/bas)
	}
}
