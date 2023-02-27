package configs

import "time"

// //// System parameters //////
const (
	FlockDefaultTimeout time.Duration = 50 * time.Millisecond
	FlockMaximumRetry   time.Duration = 10
	NumberOfReplicas    int           = 3
)

// //// Debugging parameters //////
const (
	ShowDebugInfo              = false
	ShowWarnings               = false || ShowDebugInfo
	ShowTestInfo               = false || ShowDebugInfo
	ShowRobustnessLevelChanges = false || ShowDebugInfo
	SpeedTestBatchPerThread    = 10000
)

// //// ACP related status codes ///////
const (
	PreRead      string = "[msg, status] pre-read message/status"
	PreWrite     string = "[msg, status] ore-write message/status"
	FLACProposed string = "[msg, status] FLAC propose phase"
	Commit       string = "[msg, status] transaction is committed"
	Abort        string = "[msg, status] transaction is aborted"
	PreCommit    string = "[msg, status] 3PC agreement on commit"
	Finished     string = "transaction finished"

	ReadUnsuccessful string = "[msg] ACK message for unsuccessful pre-read"
	ReadSuccess      string = "[msg] ACK message for successful pre-read"
	PreWriteACK      string = "[msg] pre-write response message"
	FLACResults      string = "[msg] the flac result message for the propose phase (vote, decision)"
	FLACGossipVotes  string = "[msg] the gossip message carrying FLAC votes"
	PreCommitACK     string = "[msg] ACK message fore pre commit message"
	// ThreePC : PreRead -> PreWrite -> AgCommitted/AgAborted -> Committed/Aborted -> Finished
	ThreePC = "3PC"
	// TwoPC : PreRead -> PreWrite -> Committed/Aborted -> Finished
	TwoPC = "2PC"
	// FLAC : PreRead -> Propose -> Committed/Aborted -> Finished
	FLAC       = "FLAC"
	PAC        = "C-PAC"
	EasyCommit = "EasyCommit"
	GPAC       = "G-PAC"
)

// //// Parameters need to be tuned according to your system ///////
const (
	KVConcurrencyDelay      = time.Millisecond / 2
	OptEps                  = 1 * time.Millisecond
	BaseGossipMsgTimeWindow = time.Duration(1.2 * 10 * float64(time.Millisecond))
	LockTimeout             = time.Millisecond / 2
	ACPRetry4NoResponse     = 3
	ResponseCheckNumber     = time.Duration(5)
	DetectorDownBatchSize   = 5
	WarmUpTime              = 4 * time.Second
	SeverAutoShutdown       = 20 * time.Second
	Retry4Abort             = true
	SimulateClientSideDelay = true
	NFTime                  = 50 * time.Millisecond
	TPCRecordPerShard       = 200000
	YCSBRecordPerShard      = 10000
	BTreeOrder              = 16
)

// //// Workload (could be changed by args) ///////
var (
	EnableReplication       = true
	NumberOfRecordsPerShard = 10000
	NumberOfShards          = 3
	TransactionLength       = 5
	YCSBDataSkewness        = 0.8
	CrossShardTXNPercentage = 100
	ClientRoutineNumber     = 10
	GossipTimeoutWindow     = time.Duration(0)
	SelectedACP             = "FLAC"
	ClientContentionDelay   = time.Duration(0)
	LockModule              = 1 // 0 for WO, 1 for WR.
	ConfigFileLocation      = "./configs/network.json"
	BasicWaitTime           = time.Duration(11 * time.Millisecond)
	FLACMinRobustnessLevel  = 2
	DetectorInitWaitCnt     = 0
	NetworkFailureInterval  = -1
	NetworkBuffer           = float64(0)
	GlobalServerTimeOut     = 20
)

func SetR(r float64) {
	NetworkBuffer = r
}
func SetBasicT(t float64) {
	BasicWaitTime = time.Duration(t * float64(time.Millisecond))
	SetMsgDelay4FLAC(NetworkBuffer)
}
func SetMsgDelay4FLAC(r float64) {
	GossipTimeoutWindow = time.Duration((r + 0.1) * float64(BasicWaitTime))
}
func SetDown(d int) {
	DetectorInitWaitCnt = d
}
func SetNF(d int) {
	NetworkFailureInterval = d
}
func SetMinLevel(l int) {
	FLACMinRobustnessLevel = l
}
func SetServerTimeOut(val int) {
	GlobalServerTimeOut = val
}
func SetConcurrency(con int) {
	ClientRoutineNumber = con
	cur := ClientRoutineNumber
	ClientContentionDelay = 1 * time.Millisecond * time.Duration(Min(cur, 5))
}

// //// DO NOT MODIFY HERE, global variables for test ///////
var (
	TestCF int32 = 0
	TestNF int32 = 0
)

func SetProtocol(pro string) {
	if pro == "2pc" {
		SelectedACP = TwoPC
	} else if pro == "3pc" {
		SelectedACP = ThreePC
	} else if pro == "flac" {
		SelectedACP = FLAC
	} else if pro == "pac" {
		SelectedACP = PAC
	} else if pro == "easy" {
		SelectedACP = EasyCommit
	} else if pro == "gpac" {
		SelectedACP = GPAC
	} else {
		SelectedACP = TwoPC
	}
}
