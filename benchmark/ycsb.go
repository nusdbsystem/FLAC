package benchmark

import (
	"FLAC/configs"
	"FLAC/coordinator"
	"FLAC/opt"
	"FLAC/participant"
	"FLAC/utils"
	"github.com/pingcap/go-ycsb/pkg/generator"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"
)

type YCSBStmt struct {
	stat *utils.Stat
	ca   *coordinator.Context
	co   []*participant.Context
	stop int32
}

type YCSBClient struct {
	md         int
	from       *YCSBStmt
	r          *rand.Rand
	zip        *generator.Zipfian
	RetryQueue []*coordinator.TX
}

func (c *YCSBClient) generateTxnKVpairs(parts []string, TID uint64) []opt.TXOpt {
	NUM_ELEMENTS := 10000
	res := make([]opt.TXOpt, 0)
	var j, val int
	md := c.md
	isCross := rand.Intn(100) < configs.CrossShardTXNPercentage
	jt := random(0, configs.NumberOfShards-1)

	if configs.TransactionLength == 0 {
		// special case: all participants one operation.
		var key uint64
		for j = 0; j < configs.NumberOfShards; j++ {
			key = uint64(c.zip.Next(c.r))
			/* Access the key from different partitions */
			configs.TPrintf("TXN" + strconv.FormatUint(TID, 10) + ": " + strconv.Itoa(j) + "[" + strconv.FormatUint(key, 10) + "] = " + strconv.Itoa(val))

			res = append(res, opt.TXOpt{
				Type:  opt.UpdateOpt,
				Shard: parts[j],
				Key:   key,
				Value: val,
			})
		}
		for i := 0; i < configs.NumberOfShards/3*2; i++ {
			j = random(0, configs.NumberOfShards-1)
			val = rand.Intn(NUM_ELEMENTS)
			key = uint64(c.zip.Next(c.r))
			/* Access the key from different partitions */
			configs.TPrintf("TXN" + strconv.FormatUint(TID, 10) + ": " + strconv.Itoa(j) + "[" + strconv.FormatUint(key, 10) + "] = " + strconv.Itoa(val))

			res = append(res, opt.TXOpt{
				Type:  opt.UpdateOpt,
				Shard: parts[j],
				Key:   key,
				Value: val,
			})
		}
	} else {
		var key uint64
		for i := 0; i < configs.TransactionLength; i++ {
			/* Code for contentious key selection */
			j = random(0, configs.NumberOfShards-1)
			val = rand.Intn(NUM_ELEMENTS)
			if i < configs.NumberOfShards && isCross {
				/* Ensure txn spans all partitions */
				j = (i%3 + md) % configs.NumberOfShards
				val = configs.ZeroValue + 1
			} else if !isCross {
				// single shard transaction
				j = jt
			} else {
				j = (j%3 + md) % configs.NumberOfShards
			}
			key = uint64(c.zip.Next(c.r))
			/* Access the key from different partitions */
			configs.TPrintf("TXN" + strconv.FormatUint(TID, 10) + ": " + strconv.Itoa(j) + "[" + strconv.FormatUint(key, 10) + "] = " + strconv.Itoa(val))

			res = append(res, opt.TXOpt{
				Type:  opt.UpdateOpt,
				Shard: parts[j],
				Key:   key,
				Value: val,
			})
		}
	}

	return res
}

func (c *YCSBClient) performTransactions(TID uint64, participants []string, txn *coordinator.TX, stats *utils.Stat) (bool, *coordinator.TX) {
	defer configs.TimeTrack(time.Now(), "performTransactions", TID)
	if txn == nil {
		kvData := c.generateTxnKVpairs(participants, TID)
		exist := make(map[string]bool)
		parts := make([]string, 0)
		for _, v := range kvData {
			sd := v.Shard
			if exist[sd] == false {
				exist[sd] = true
				parts = append(parts, sd)
			}
		}
		txn = coordinator.NewTX(TID, 0, parts, c.from.ca.Manager)
		txn.OptList = kvData
	} else {
		txn.TxnID = TID
	}
	info := utils.NewInfo(len(txn.Participants))
	c.from.ca.Manager.SubmitTxn(txn, configs.SelectedACP, info)
	stats.Append(info)
	return info.IsCommit, txn
}

func (stmt *YCSBStmt) Stopped() bool {
	return atomic.LoadInt32(&stmt.stop) != 0
}

func (stmt *YCSBStmt) startYCSBClient(seed int, md int) {
	client := YCSBClient{md: md, from: stmt}
	client.RetryQueue = make([]*coordinator.TX, 0)

	client.r = rand.New(rand.NewSource(int64(seed)*11 + 31))
	client.zip = generator.NewZipfianWithRange(0, int64(configs.NumberOfRecordsPerShard-2), configs.YCSBDataSkewness)
	for !stmt.Stopped() {
		TID := utils.GetTxnID()
		client.performTransactions(TID, configs.OuAddress, nil, stmt.stat)
	}
}

func (stmt *YCSBStmt) Stop() {
	stmt.ca.Close()
	atomic.StoreInt32(&stmt.stop, 1)
	if stmt.co == nil {
		return
	}
	for _, v := range stmt.co {
		v.Close()
	}
}

func (stmt *YCSBStmt) YCSBTest() {
	if configs.LocalTest {
		stmt.ca, stmt.co = coordinator.BatchTestKit()
	} else {
		stmt.ca = coordinator.RemoteTestkit("10.10.10.226:2001")
		stmt.co = nil
	}
	//	atomic.StoreInt32(&stmt.stop, 0)
	stmt.stat = utils.NewStat()
	rand.Seed(1234)
	for i := 0; i < configs.ClientRoutineNumber; i++ {
		go stmt.startYCSBClient(i*11+13, i)
	}
	configs.TPrintf("All clients Started")
	time.Sleep(configs.WarmUpTime)
	stmt.stat.Clear()
	time.Sleep(2 * time.Second)
	stmt.stat.Log()
	stmt.stat.Clear()
}
