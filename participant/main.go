package participant

import (
	"FLAC/configs"
	"encoding/json"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Context records the statement context for Manager nodes.
type Context struct {
	mu              *sync.Mutex
	timeoutForMsg   time.Duration
	timeoutForLocks time.Duration
	coordinator     string
	participants    []string
	address         string

	Manager *Manager // the participant manager

	done chan bool
	conn *Commu
}

var conLock = sync.Mutex{}
var config map[string]interface{}

func initData(stmt *Context, service string) {
	loadConfig(stmt, &config)
	configs.TPrintf("Load config finished")
	stmt.mu = &sync.Mutex{}
	stmt.address = service
	storageSize := configs.NumberOfRecordsPerShard
	stmt.Manager = NewParticipantManager(stmt, storageSize)
}

func loadConfig(stmt *Context, config *map[string]interface{}) {
	conLock.Lock()
	defer conLock.Unlock()
	/* Read the config file and store it in 'config' variable */
	raw, err := os.ReadFile(configs.ConfigFileLocation)
	if err != nil {
		raw, err = os.ReadFile("." + configs.ConfigFileLocation)
	}
	configs.CheckError(err)

	err = json.Unmarshal(raw, &config)
	tmp, _ := ((*config)["participants"]).(map[string]interface{})
	stmt.participants = make([]string, 0)
	idKey := "1"
	for i, p := range tmp {
		tp, err := strconv.Atoi(i)
		configs.CheckError(err)
		if tp <= configs.NumberOfShards {
			stmt.participants = append(stmt.participants, p.(string))
		}
	}
	sort.Strings(stmt.participants)
	if len(configs.OuAddress) == 0 {
		configs.OuAddress = stmt.participants
	}
	tmp, _ = ((*config)["coordinators"]).(map[string]interface{})
	for _, p := range tmp {
		stmt.coordinator = p.(string)
	}
	tmp, _ = ((*config)["delays"]).(map[string]interface{})
	for i, p := range tmp {
		if i == idKey {
			configs.SetBasicT(p.(float64))
		}
	}
	stmt.timeoutForLocks = configs.LockTimeout
	stmt.timeoutForMsg = configs.GossipTimeoutWindow
	stmt.done = make(chan bool, 1)
	configs.CheckError(err)
}

// Close the running participant process.
func (stmt *Context) Close() {
	configs.TPrintf("Close called!!! at " + stmt.address)
	stmt.done <- true
	stmt.conn.Stop()
}

func begin(stmt *Context, ch chan bool, service string) {
	configs.TPrintf("Initializing -- ")
	initData(stmt, service)
	configs.DPrintf(service)
	stmt.conn = NewConns(stmt, service)

	configs.DPrintf("build finished for " + service)

	go func() {
		time.Sleep(configs.SeverAutoShutdown + configs.WarmUpTime)
		stmt.Close()
	}()

	stmt.activatePeriodicCrashFailure()
	stmt.activatePeriodicNetworkFailure()
	ch <- true
	stmt.Run()
}

// Main the main function for a participant process.
func Main(preload bool, addr string) {
	stmt := &Context{}
	ch := make(chan bool)
	go func() {
		<-ch
		if preload {
			stmt.LoadStock()
		}
	}()
	begin(stmt, ch, addr)
}

func (stmt *Context) activatePeriodicCrashFailure() {
	if configs.GlobalServerTimeOut <= 0 && (stmt.address == "127.0.0.1:6001" || !configs.LocalTest) {
		// for test, to simulate the jerky environments.
		if configs.GlobalServerTimeOut == 0 {
			stmt.Manager.Break()
		} else {
			go func() {
				for {
					time.Sleep(time.Duration(-configs.GlobalServerTimeOut) * time.Millisecond)
					stmt.Manager.Break()
					time.Sleep(time.Duration(-configs.GlobalServerTimeOut) * time.Millisecond)
					stmt.Manager.Recover()
				}
			}()
		}
	}
}

func (stmt *Context) activatePeriodicNetworkFailure() {
	if configs.NetworkFailureInterval >= 0 && (stmt.address == "127.0.0.1:6001" || !configs.LocalTest) {
		if configs.NetworkFailureInterval == 0 {
			stmt.Manager.NetBreak()
		} else {
			go func() {
				for {
					time.Sleep(time.Duration(configs.NetworkFailureInterval) * time.Millisecond)
					stmt.Manager.NetBreak()
					time.Sleep(time.Duration(configs.NetworkFailureInterval) * time.Millisecond)
					stmt.Manager.NetRecover()
				}
			}()
		}
	}
}

func (stmt *Context) Run() {
	stmt.conn.Run()
}

func (stmt *Context) GetAddr() string {
	return stmt.address
}
