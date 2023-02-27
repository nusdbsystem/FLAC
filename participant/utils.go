package participant

import (
	"FLAC/configs"
	"FLAC/utils"
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

func comTestStart() []*Context {
	stmts := make([]*Context, configs.NumberOfShards)
	ch := make(chan bool)
	for i := 0; i < configs.NumberOfShards; i++ {
		stmts[i] = &Context{}
		go begin(stmts[i], ch, configs.OuAddress[i])
		<-ch
	}
	return stmts
}

// OUParticipantsTestKit For test
func OUParticipantsTestKit() []*Context {
	stmts := comTestStart()
	for i := 0; i < configs.NumberOfShards; i++ {
		for j := 0; j < 5; j++ {
			stmts[i].Manager.Kv[stmts[i].Manager.stmt.address].Update(uint64(j), j)
		}
	}
	return stmts
}

func OUParticipantsTestKitBatch() []*Context {
	stmts := comTestStart()
	for i := 0; i < configs.NumberOfShards; i++ {
		for j := 0; j < configs.NumberOfRecordsPerShard; j++ {
			stmts[i].Manager.Kv[stmts[i].Manager.stmt.address].Update(uint64(j), j)
		}
	}
	return stmts
}

func OUParticipantsTestKitTPC() []*Context {
	stmts := comTestStart()
	for i := 0; i < configs.NumberOfShards; i++ {
		stmts[i].LoadStock()
		time.Sleep(10 * time.Millisecond)
	}
	return stmts
}

var stockLock = sync.Mutex{}

// LoadStock load the history data of each warehouse for TPC-C benchmark
func (stmt *Context) LoadStock() {
	stockLock.Lock()
	defer stockLock.Unlock()
	file, err := os.Open("./benchmark/data/stock.csv")
	if err != nil {
		file, err = os.Open("../benchmark/data/stock.csv")
	}
	configs.CheckError(err)
	defer file.Close()
	s := csv.NewReader(file)
	cnt := 0
	for {
		row, err := s.Read()
		if err == io.EOF {
			break
		}
		item, err := strconv.Atoi(row[0])
		configs.CheckError(err)
		ware, err := strconv.Atoi(row[1])
		configs.CheckError(err)
		count, err := strconv.Atoi(row[2])
		configs.CheckError(err)
		ware--
		if configs.OuAddress[ware] == stmt.address {
			cnt++
			stmt.Manager.Kv[stmt.address].Update(uint64(utils.TransTableItem("stock", ware, item)), count)
		}
	}
	configs.TPrintf("Initialize the stock over")
}
