package coordinator

import (
	"FLAC/configs"
	"FLAC/detector"
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Context records the statement context for a coordinator node.
type Context struct {
	// string address -> node.
	Manager         *FLACManager
	coordinatorID   string
	participants    []string
	replicas        map[string][]string
	timeoutForMsg   time.Duration
	timeoutForLocks time.Duration
	conn            *Commu
}

var conLock = sync.Mutex{}
var config map[string]interface{}

// [] [address] [storageSize]
func initData(stmt *Context, Args []string) {
	loadConfig(stmt, &config)
	stmt.coordinatorID = Args[2]
	stmt.Manager = NewFLACManager(stmt)
}

func loadConfig(stmt *Context, config *map[string]interface{}) {
	conLock.Lock()
	defer conLock.Unlock()
	/* Read the config file and store it in 'config' variable */
	raw, err := ioutil.ReadFile(configs.ConfigFileLocation)
	if err != nil {
		raw, err = ioutil.ReadFile("." + configs.ConfigFileLocation)
	}
	configs.CheckError(err)

	err = json.Unmarshal(raw, &config)
	tmp, _ := ((*config)["participants"]).(map[string]interface{})
	stmt.participants = make([]string, 0)
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
		stmt.coordinatorID = p.(string)
	}
	tmp, _ = ((*config)["delays"]).(map[string]interface{})
	for i, p := range tmp {
		if i == "1" {
			configs.SetBasicT(p.(float64))
		}
	}
	stmt.replicas = make(map[string][]string)
	if configs.EnableReplication {
		// cycle replication: i replicated to i+1, i+2, i+R mod N.
		for i := 0; i < configs.NumberOfShards; i++ {
			for j := 0; j < configs.NumberOfReplicas; j++ {
				stmt.replicas[stmt.participants[i]] = append(stmt.replicas[stmt.participants[i]],
					stmt.participants[(i+j)%configs.NumberOfShards])
			}
		}
	}
	stmt.timeoutForLocks = configs.LockTimeout
	stmt.timeoutForMsg = configs.BaseGossipMsgTimeWindow
	configs.CheckError(err)
}

func (c *Context) Close() {
	detector.Stop()
	c.conn.Close()
}

func begin(stmt *Context, Args []string, ch chan bool) {
	initData(stmt, Args)

	service := Args[2]
	configs.DPrintf(service)
	stmt.conn = NewConns(stmt, service)
	ch <- true
	stmt.conn.Run()
}

func Main() {
	stmt := &Context{}
	ch := make(chan bool)
	begin(stmt, os.Args, ch)
}
