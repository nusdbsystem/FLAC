package utils

import (
	"FLAC/configs"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Stat struct {
	mu       *sync.Mutex
	txnInfos []*Info
	beginTS  int
	endTS    int
}

func NewStat() *Stat {
	res := &Stat{
		txnInfos: make([]*Info, configs.MaxTID),
		mu:       &sync.Mutex{},
		beginTS:  0,
		endTS:    0,
	}
	return res
}

func (st *Stat) Append(info *Info) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.endTS++
	st.txnInfos[st.endTS] = info
}

func (st *Stat) Log() {
	st.mu.Lock()
	defer st.mu.Unlock()
	txnCnt, cross, success, fail, crossSuc := 0, 0, 0, 0, 0
	latencySum, levelSum, s1, s2, s3 := 0, 0.0, time.Duration(0), time.Duration(0), time.Duration(0)
	latencies := make([]int, 0)
	println(st.beginTS, st.endTS)
	for i := st.beginTS; i < st.endTS; i++ {
		if st.txnInfos[i] != nil {
			tmp := st.txnInfos[i]
			txnCnt++
			if tmp.NumPart > 1 {
				cross++
				levelSum += float64(tmp.Level)
			}
			if tmp.Failure {
				fail++
			}
			if tmp.Latency > 0 {
				latencySum += int(tmp.Latency)
				latencies = append(latencies, int(tmp.Latency))
			}
			if tmp.IsCommit {
				success++
				//				latencySum += int(tmp.Latency)
				//				latencies = append(latencies, int(tmp.Latency))
				if tmp.NumPart > 1 {
					s1 += tmp.ST1
					s2 += tmp.ST2
					s3 += tmp.ST3
					crossSuc++
				}
			}
		}
	}
	msg := "count:" + strconv.Itoa(txnCnt) + ";"
	msg += "cross:" + strconv.Itoa(cross) + ";"
	msg += "concurrency:" + strconv.Itoa(configs.ClientRoutineNumber) + ";"
	msg += "success:" + strconv.Itoa(success) + ";"
	msg += "crossSuc:" + strconv.Itoa(crossSuc) + ";"
	msg += "error:" + strconv.Itoa(fail) + ";"
	sort.Ints(latencies)
	if len(latencies) > 0 {
		i := Min((len(latencies)*99+99)/100, len(latencies)-1)
		msg += "p99:" + time.Duration(time.Duration(latencies[i]).Nanoseconds()).String() + ";"
		i = Min((len(latencies)*9+9)/10, len(latencies)-1)
		msg += "p90:" + time.Duration(time.Duration(latencies[i]).Nanoseconds()).String() + ";"
		i = Min((len(latencies)+1)/2, len(latencies)-1)
		msg += "p50:" + time.Duration(time.Duration(latencies[i]).Nanoseconds()).String() + ";"
		msg += "ave:" + time.Duration(time.Duration(latencySum/len(latencies)).Nanoseconds()).String() + ";"
	} else {
		msg += "p99:nil;"
		msg += "p90:nil;"
		msg += "p50:nil;"
		msg += "ave:nil;"
	}
	if cross == 0 {
		msg += "level:nil;"
	} else {
		msg += "level:" + fmt.Sprintf("%f", levelSum/float64(cross)) + ";"
	}
	if crossSuc == 0 {
		msg += "s1:nil;"
		msg += "s2:nil;"
		msg += "s3:nil;"
	} else {
		msg += "s1:" + time.Duration(s1.Nanoseconds()/int64(crossSuc)).String() + ";"
		msg += "s2:" + time.Duration(s2.Nanoseconds()/int64(crossSuc)).String() + ";"
		msg += "s3:" + time.Duration(s3.Nanoseconds()/int64(crossSuc)).String() + ";"
	}
	fmt.Println(msg)
}

func (st *Stat) Clear() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.beginTS = st.endTS + 1
}

type Info struct {
	NumPart    int
	Failure    bool
	Level      int
	RetryCount int
	IsCommit   bool
	Latency    time.Duration
	ST1        time.Duration
	ST2        time.Duration
	ST3        time.Duration
}

func NewInfo(NPart int) *Info {
	res := &Info{
		NumPart: NPart,
		Failure: false, Level: -1, IsCommit: false, Latency: 0,
		ST1: 0, ST2: 0, ST3: 0, RetryCount: 0,
	}
	return res
}

func (c *Info) Merge(info *Info) {
	c.Failure = c.Failure || info.Failure
	c.IsCommit = c.IsCommit || info.IsCommit
	c.RetryCount++
	c.Level += info.Level
	c.Latency += info.Latency
	c.ST1 += info.ST1
	c.ST2 += info.ST2
	c.ST3 += info.ST3
}
