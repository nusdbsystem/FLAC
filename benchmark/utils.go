package benchmark

import (
	"FLAC/configs"
)

func TestYCSB(protocol string, addr string) {
	st := YCSBStmt{}
	configs.SetProtocol(protocol)
	st.YCSBTest()
	st.Stop()
}

func TestTPC(protocol string, addr string) {
	st := TPCStmt{}
	configs.SetProtocol(protocol)
	st.TPCC_Test(nil)
	st.Stop()
}
