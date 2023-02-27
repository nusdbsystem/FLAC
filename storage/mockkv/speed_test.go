package mockkv

import (
	"FLAC/configs"
	"FLAC/detector"
	"FLAC/network"
	"FLAC/opt"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func TestNoContentionWrite(t *testing.T) {
	ta := newShardKV("id", 10000)
	rand.Seed(time.Now().UnixNano())
	st := time.Now()
	for i := 0; i < 1000000; i++ {
		ta.Update(uint64(rand.Intn(1000)+1), rand.Intn(10000))
	}
	fmt.Println("No contention write/second = ", 1000000.0/float64(time.Since(st).Seconds()))
}

func TestNoContentionRead(t *testing.T) {
	ta := newShardKV("id", 10000)
	rand.Seed(time.Now().UnixNano())
	st := time.Now()
	for i := 0; i < 1000000; i++ {
		ta.Read(rand.Intn(1000) + 1)
	}
	fmt.Println("No contention read/second = ", 1000000.0/float64(time.Since(st).Seconds()))
}

func txnAccess(shard *Shard, n, rang int, readPr float64, txnID uint64) bool { // Solved because of random function!!!!!
	//time.Sleep(100 * time.Nanosecond)
	//configs.TimeTrack(time.Now(), "txnAccess in KV", txnID)
	tx := network.NewTXPack(txnID, "id", detector.NoCFNoNF, []string{"id"})
	for i := 0; i < n; i++ {
		if rand.Float64() < readPr {
			tx.AppendRead("id", uint64(rand.Intn(rang)+1))
		} else {
			tx.AppendUpdate("id", uint64(rand.Intn(rang)+1), rand.Intn(10000))
		}
	}
	shard.Begin(txnID)
	for _, v := range tx.OptList {
		if v.Type == opt.ReadOpt {
			_, ok := shard.ReadTxn(txnID, v.Key)
			if !ok {
				shard.RollBack(txnID)
				return false
			}
		} else {
			if !shard.UpdateTxn(txnID, v.Key, v.Value.(int)) {
				shard.RollBack(txnID)
				return false
			}
		}
	}
	shard.Commit(txnID)
	return true
}

func TestW4R(t *testing.T) {
	ta := newShardKV("id", 10000)
	rand.Seed(time.Now().UnixNano())
	go func() {
		for i := 0; i < 1000000; i++ {
			ta.Read(rand.Intn(100) + 1)
		}
	}()
	go func() {
		for i := 0; i < 1000000; i++ {
			ta.Read(rand.Intn(100) + 1)
		}
	}()
	st := time.Now()
	for i := 0; i < 1000000; i++ {
		ta.Update(uint64(rand.Intn(100)+1), rand.Intn(10000))
	}
	fmt.Println("Write/second with two thread accessing = ", 1000000.0/float64(time.Since(st).Seconds()))
}

func TestTxNNoContention(t *testing.T) {
	ta := newShardKV("id", 10000)
	rand.Seed(time.Now().UnixNano())
	st := time.Now()
	suc := 0
	for i := uint64(0); i < 10000; i++ {
		if txnAccess(ta, 2, 200, 0.2, i) {
			suc++
		}
	}
	bas := float64(time.Since(st).Seconds())
	fmt.Println("txn/second without contention", 10000.0/bas, float64(suc)/bas)
}

func TestTxNConcurrent(t *testing.T) {
	ta := newShardKV("id", configs.NumberOfRecordsPerShard)
	rand.Seed(time.Now().UnixNano())
	for con := 1; con < 8; con++ {
		st := time.Now()
		suc := int32(0)
		ch := make(chan bool, con)
		for c := 0; c < con; c++ {
			go func(done chan bool, pos int) {
				for i := 0; i < configs.SpeedTestBatchPerThread; i++ {
					tid := uint64(i + configs.SpeedTestBatchPerThread*pos)
					if txnAccess(ta, 5, configs.NumberOfRecordsPerShard-1, 0, tid) {
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
