package main

import (
	"FLAC/benchmark"
	"FLAC/configs"
	"FLAC/participant"
	"flag"
)

var (
	part     string
	protocol string
	numPart  int
	l        int
	con      int
	tl       int
	nf       int
	down     int
	minLevel int
	cross    int
	bench    string
	local    bool
	preload  bool
	r        float64
	sk       float64
	addr     string
)

func usage() {
	flag.PrintDefaults()
}

func init() {
	// Common
	flag.IntVar(&numPart, "part", 3, "the number of participants")
	flag.IntVar(&l, "len", 5, "the transaction length")
	flag.StringVar(&addr, "addr", "127.0.0.1:5001", "the address for this node")
	flag.StringVar(&part, "node", "ca", "the node to start")
	flag.Float64Var(&r, "r", 1, "the factor for the FLAC protocol")
	flag.Float64Var(&sk, "skew", 0.4, "the skew factor for ycsb zipf")
	flag.IntVar(&cross, "cross", 100, "the cross shard transaction rate.")

	// For coordinator
	flag.StringVar(&bench, "bench", "tpc", "the benchmark used for the test")
	flag.StringVar(&protocol, "p", "flac", "the protocol used for this test")
	flag.IntVar(&con, "c", 1000, "the number of client used for test")
	flag.IntVar(&down, "d", 1, "The heuristic method used: x for fixed timeout, 0 for RL.")
	flag.BoolVar(&local, "local", false, "if the test is executed locally")

	// For participant.
	flag.IntVar(&tl, "tl", 28, "the timeout for started participant node, -x for change, 0 for crash")
	flag.IntVar(&nf, "nf", -1, "the interval for network failure")
	flag.IntVar(&minLevel, "ml", 0, "The smallest level can be used.")
	// if ml < 0 then fix the level, -1 for ff, -2 for cf, -3 for nf
	flag.BoolVar(&preload, "preload", false, "preload the data for tpc-c into shard")

	flag.Usage = usage
}

func main() {
	//defer profile.Start(profile.ProfilePath("./profile/")).Close()
	flag.Parse()
	//	println(con, r, preload, local, protocol, bench, part)
	configs.NumberOfShards = numPart
	configs.SetConcurrency(con)
	configs.SetR(r)
	configs.SetServerTimeOut(tl)
	if down > 0 {
		configs.SetDown(down)
	} else {
		configs.SetDown(0)
	}
	configs.CrossShardTXNPercentage = cross
	configs.YCSBDataSkewness = sk
	configs.TransactionLength = l
	configs.SetNF(nf)
	configs.SetMinLevel(minLevel)
	if local {
		configs.SetLocal()
	}

	if part == "co" {
		configs.NumberOfRecordsPerShard = configs.TPCRecordPerShard
		participant.Main(preload, addr)
	} else if part == "ca" {
		if bench == "ycsb" {
			configs.NumberOfRecordsPerShard = configs.YCSBRecordPerShard
			benchmark.TestYCSB(protocol, addr)
		} else if bench == "tpc" {
			configs.NumberOfRecordsPerShard = configs.TPCRecordPerShard
			benchmark.TestTPC(protocol, addr)
		}
	}
}
