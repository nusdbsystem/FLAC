package configs

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
)

var OuAddress = []string{}
var LocalTest = false

func SetLocal() {
	LocalTest = true
	ConfigFileLocation = "./configs/local.json"
}

func TxnPrint(tid uint64, format string, a ...interface{}) {
	if ShowDebugInfo {
		fmt.Printf(time.Now().Format("15:04:05.00")+" <---> "+"TXN"+strconv.FormatUint(tid, 10)+":"+format+"\n", a...)
	}
}

func DPrintf(format string, a ...interface{}) {
	if ShowDebugInfo {
		fmt.Printf(time.Now().Format("15:04:05.00")+" <---> "+format+"\n", a...)
	}
	return
}

func TimeTrack(start time.Time, name string, TID uint64) {
	tim := time.Since(start).String()
	if name == "FLAC ST1" {
		//	fmt.Println("1:" + tim)
	} else if name == "FLAC Decide" {
		//	fmt.Println("2:" + tim)
	}
	TPrintf("TXN" + strconv.FormatUint(TID, 10) + ": Time cost for " + name + " : " + tim)
}

func TimeAdd(start time.Time, name string, TID uint64, latency *time.Duration) {
	if latency == nil {
		return
	}
	*latency = time.Since(start) + *latency
	TPrintf("TXN" + strconv.FormatUint(TID, 10) + ": Time cost for " + name + " : " + (*latency).String())
}

func TimeLoad(start time.Time, name string, TID uint64, latency *time.Duration) {
	if latency == nil {
		return
	}
	*latency = time.Since(start)
	TPrintf("TXN" + strconv.FormatUint(TID, 10) + ": Time cost for " + name + " : " + (*latency).String())
}

func LPrintf(format string, a ...interface{}) {
	if ShowRobustnessLevelChanges {
		fmt.Printf(time.Now().Format("15:04:05.00")+" <---> "+format+"\n", a...)
	}
	return
}

func TPrintf(format string, a ...interface{}) {
	if ShowTestInfo {
		fmt.Printf(time.Now().Format("15:04:05.00")+" <---> "+format+"\n", a...)
	}
	return
}

func JPrint(v interface{}) {
	byt, _ := json.Marshal(v)
	fmt.Println(string(byt))
}

func Hash(shard string, key uint64) string {
	return shard + "_" + strconv.FormatUint(key, 10)
}

func Assert(cond bool, msg string) bool {
	if !cond {
		panic("[ERROR] Assert error at " + msg + "\n")
	}
	return cond
}

func Warn(cond bool, msg string) bool {
	if ShowWarnings && !cond {
		fmt.Printf("[WARNNING] :" + msg + "\n")
	}
	return cond
}
func CheckError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
